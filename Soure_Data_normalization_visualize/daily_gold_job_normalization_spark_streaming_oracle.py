# -*- coding: utf-8 -*-
"""
Daily Gold ETL Job - Spark Structured Streaming với Oracle Polling
Tự động phát hiện và xử lý khi Oracle database thay đổi

Giải pháp: Spark Structured Streaming + foreachBatch
- Dùng memory source làm trigger
- foreachBatch để polling Oracle mỗi interval
- Có checkpoint tự động, recovery, monitoring
"""

import argparse
import datetime as dt
import os
import sys
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    col, when, lit, trim, upper, lower, regexp_replace, 
    concat_ws, first, last, max as spark_max, min as spark_min,
    count, isnan, isnull, coalesce, to_timestamp, date_format,
    row_number, window, monotonically_increasing_id, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    TimestampType, DoubleType, LongType
)
from pyspark.sql.window import Window

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz

import re
import unicodedata

# Import các hàm clean từ batch job
# Thêm đường dẫn để import
sys.path.insert(0, os.path.dirname(__file__))
try:
    from daily_gold_job_normalization_spark import (
        normalize_locations,
        enrich_gold_types,
        normalize_purity_format,
        normalize_category_smart,
        normalize_gold_type_and_unit,
        merge_duplicate_types_and_update_fact,
        build_similarity_groups,
        norm_txt,
        snapshot_table
    )
    BATCH_FUNCTIONS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Không thể import batch functions: {e}")
    print("   Sẽ chỉ xử lý FACT, không clean LOCATION và TYPE")
    BATCH_FUNCTIONS_AVAILABLE = False

# ====================== CONFIG ======================
# Đọc từ environment variables (Docker) hoặc dùng giá trị mặc định
DB_USER = os.environ.get("DB_USER", "SYSTEM")
DB_PASS = os.environ.get("DB_PASS", "Welcome_1234")
DB_HOST = os.environ.get("DB_HOST", "136.110.60.196")
DB_PORT = os.environ.get("DB_PORT", "1521")
DB_SERVICE = os.environ.get("DB_SERVICE", "XEPDB1")

DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
DB_URL = f"jdbc:oracle:thin:@{DB_DSN}"

SNAPSHOT_DIR = "./snapshots"
JOB_NAME = "DAILY_GOLD_JOB_STREAMING_ORACLE"
SIM_THRESHOLD_LOC = 0.80
SIM_THRESHOLD_TYPE = 0.75
FUZZY_FALLBACK = 90

# Các constants cần thiết cho batch functions (nếu import được)
try:
    from daily_gold_job_normalization_spark import (
        SIM_THRESHOLD_LOC as BATCH_SIM_THRESHOLD_LOC,
        SIM_THRESHOLD_TYPE as BATCH_SIM_THRESHOLD_TYPE,
        FUZZY_FALLBACK as BATCH_FUZZY_FALLBACK
    )
except:
    pass

# Streaming config
STREAMING_CHECKPOINT_DIR = "./checkpoints/streaming_oracle"
STREAMING_TRIGGER_INTERVAL = "60 seconds"  # Polling mỗi 60 giây
TIMESTAMP_COLUMN = "RECORDED_AT"  # Cột timestamp để phát hiện thay đổi

# Spark config
SPARK_APP_NAME = "DailyGoldETLJobStreamingOracle"
SPARK_MASTER = "local[*]"

# ====================================================

def create_spark_session(ojdbc_path: str = None):
    """Tạo SparkSession với cấu hình streaming."""
    builder = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.streaming.checkpointLocation", STREAMING_CHECKPOINT_DIR)
    
    # Thêm JDBC driver nếu có
    if ojdbc_path:
        if os.path.exists(ojdbc_path):
            builder = builder.config("spark.jars", ojdbc_path)
            print(f"✅ Đã load JDBC driver từ: {ojdbc_path}")
        else:
            print(f"⚠️ Không tìm thấy JDBC driver tại: {ojdbc_path}")
    else:
        possible_paths = [
            "ojdbc8.jar",
            "./ojdbc8.jar",
            "../ojdbc8.jar",
            os.path.join(os.path.dirname(__file__), "ojdbc8.jar")
        ]
        for path in possible_paths:
            if os.path.exists(path):
                builder = builder.config("spark.jars", os.path.abspath(path))
                print(f"✅ Đã tự động tìm thấy JDBC driver: {os.path.abspath(path)}")
                break
    
    spark = builder.getOrCreate()
    return spark

def read_table_from_oracle(spark: SparkSession, table_name: str, schema: str = None) -> 'DataFrame':
    """Đọc bảng từ Oracle DB (batch)."""
    schema_prefix = f'"{schema}"."' if schema else '"'
    full_table = f'{schema_prefix}{table_name}"'
    
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", full_table) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    return df

def read_new_data_from_oracle(spark: SparkSession, table_name: str, 
                              last_timestamp: dt.datetime,
                              timestamp_column: str = TIMESTAMP_COLUMN) -> 'DataFrame':
    """
    Đọc chỉ dữ liệu MỚI từ Oracle dựa trên timestamp.
    """
    schema_prefix = f'"{DB_USER}"."'
    full_table = f'{schema_prefix}{table_name}"'
    
    # Tạo query để chỉ lấy dữ liệu mới
    ts_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        (SELECT * FROM {full_table}
         WHERE {timestamp_column} > TO_TIMESTAMP('{ts_str}', 'YYYY-MM-DD HH24:MI:SS')
         ORDER BY {timestamp_column})
    """
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
            .option("dbtable", query) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        return df
    except Exception as e:
        print(f"⚠️ Lỗi khi đọc dữ liệu mới: {e}")
        return spark.createDataFrame([], get_fact_schema())

def get_last_timestamp_from_checkpoint(spark: SparkSession) -> dt.datetime:
    """Lấy timestamp cuối cùng từ checkpoint."""
    try:
        df = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        df_checkpoint = df.filter(col("JOB_NAME") == JOB_NAME)
        
        if df_checkpoint.count() > 0:
            last_run = df_checkpoint.select("LAST_RUN").first()
            if last_run and last_run[0]:
                return last_run[0]
    except Exception as e:
        print(f"⚠️ Không đọc được checkpoint: {e}")
    
    # Nếu chưa có checkpoint, lấy timestamp từ bảng FACT
    try:
        df_fact = read_table_from_oracle(spark, "GOLD_PRICE_FACT", DB_USER)
        if df_fact.count() > 0:
            max_ts = df_fact.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
            if max_ts:
                return max_ts
    except Exception as e:
        print(f"⚠️ Không lấy được timestamp từ FACT: {e}")
    
    return dt.datetime(2000, 1, 1)

def update_checkpoint(spark: SparkSession, ts: dt.datetime):
    """Cập nhật checkpoint."""
    checkpoint_df = spark.createDataFrame(
        [(JOB_NAME, ts)],
        ["JOB_NAME", "LAST_RUN"]
    )
    
    try:
        existing = read_table_from_oracle(spark, "ETL_CHECKPOINT", DB_USER)
        combined = existing.filter(col("JOB_NAME") != JOB_NAME).union(checkpoint_df)
    except:
        combined = checkpoint_df
    
    combined.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", f"{DB_USER}.ETL_CHECKPOINT") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode("overwrite") \
        .save()

def write_table_to_oracle(df: 'DataFrame', table_name: str, mode: str = "append"):
    """Ghi DataFrame vào Oracle DB."""
    if df.count() == 0:
        return
    
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:{DB_USER}/{DB_PASS}@{DB_DSN}") \
        .option("dbtable", table_name) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode(mode) \
        .save()

def get_fact_schema():
    """Schema cho GOLD_PRICE_FACT."""
    return StructType([
        StructField("SOURCE_ID", IntegerType(), True),
        StructField("TYPE_ID", IntegerType(), True),
        StructField("LOCATION_ID", IntegerType(), True),
        StructField("TIME_ID", IntegerType(), True),
        StructField("BUY_PRICE", DoubleType(), True),
        StructField("SELL_PRICE", DoubleType(), True),
        StructField("RECORDED_AT", TimestampType(), True),
        StructField("UNIT", StringType(), True),
    ])

# ==================== PROCESSING FUNCTIONS ====================

def process_new_fact_data(spark: SparkSession, df_new: 'DataFrame',
                          location_mapping: Dict, type_mapping: Dict) -> 'DataFrame':
    """
    Xử lý dữ liệu FACT mới:
    1. Apply location/type mappings
    2. Deduplicate
    3. Handle missing values
    4. Flag outliers
    """
    if df_new.count() == 0:
        return df_new
    
    # Apply location mapping
    if location_mapping:
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in location_mapping.items()],
            ["OLD_LOC_ID", "NEW_LOC_ID"]
        )
        df_new = df_new.join(
            mapping_df,
            df_new["LOCATION_ID"] == mapping_df["OLD_LOC_ID"],
            "left"
        ).withColumn(
            "LOCATION_ID",
            when(col("NEW_LOC_ID").isNotNull(), col("NEW_LOC_ID"))
            .otherwise(col("LOCATION_ID"))
        ).drop("OLD_LOC_ID", "NEW_LOC_ID")
    
    # Apply type mapping
    if type_mapping:
        mapping_df = spark.createDataFrame(
            [(k, v) for k, v in type_mapping.items()],
            ["OLD_TYPE_ID", "NEW_TYPE_ID"]
        )
        df_new = df_new.join(
            mapping_df,
            df_new["TYPE_ID"] == mapping_df["OLD_TYPE_ID"],
            "left"
        ).withColumn(
            "TYPE_ID",
            when(col("NEW_TYPE_ID").isNotNull(), col("NEW_TYPE_ID"))
            .otherwise(col("TYPE_ID"))
        ).drop("OLD_TYPE_ID", "NEW_TYPE_ID")
    
    # Deduplicate (giữ record mới nhất)
    df_new = df_new.withColumn(
        "COMBO",
        concat_ws("|",
            col("SOURCE_ID").cast("string"),
            col("TYPE_ID").cast("string"),
            col("LOCATION_ID").cast("string"),
            col("TIME_ID").cast("string")
        )
    )
    
    window_spec = Window.partitionBy("COMBO").orderBy(col(TIMESTAMP_COLUMN).desc())
    df_new = df_new.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "COMBO")
    
    # Handle missing values
    df_new = df_new.filter(
        col("BUY_PRICE").isNotNull() &
        col("SELL_PRICE").isNotNull() &
        col(TIMESTAMP_COLUMN).isNotNull()
    )
    
    # Flag outliers
    df_new = df_new.withColumn("IS_DELETED", lit(0))
    df_new = df_new.withColumn("IS_DELETE", lit(0))
    
    return df_new

def load_dimension_mappings(spark: SparkSession) -> Tuple[Dict, Dict]:
    """Load location và type mappings từ CLEAN tables."""
    location_mapping = {}
    type_mapping = {}
    
    # TODO: Implement logic để load mappings
    # Có thể load từ LOCATION_DIMENSION_CLEAN và GOLD_TYPE_DIMENSION_CLEAN
    
    return location_mapping, type_mapping

# ==================== STREAMING WITH FOREACHBATCH ====================

def clean_all_dimensions_incremental(spark: SparkSession, merge_types: bool = False) -> Tuple[Dict, Dict]:
    """
    Clean tất cả dimension tables (LOCATION và TYPE) - INCREMENTAL.
    Giữ nguyên dữ liệu CLEAN cũ, chỉ cập nhật/thêm mới.
    Trả về mappings để dùng cho FACT.
    """
    if not BATCH_FUNCTIONS_AVAILABLE:
        print("⚠️ Không thể clean dimensions, chỉ dùng mappings hiện có")
        return {}, {}
    
    print("\n" + "="*60)
    print("🧹 Đang clean tất cả dimension tables (INCREMENTAL)...")
    print("="*60)
    
    # B1: LOCATION normalize -> LOCATION_DIMENSION_CLEAN
    print("\n📍 Bước 1: Normalize LOCATION_DIMENSION...")
    
    # Đọc dữ liệu CLEAN hiện có TRƯỚC (để giữ lại)
    try:
        df_loc_clean_existing = read_table_from_oracle(spark, "LOCATION_DIMENSION_CLEAN", DB_USER)
        existing_loc_count = df_loc_clean_existing.count()
        existing_loc_ids = set([row["ID"] for row in df_loc_clean_existing.select("ID").collect()])
        print(f"📊 LOCATION_CLEAN hiện có: {existing_loc_count} records")
    except:
        df_loc_clean_existing = None
        existing_loc_ids = set()
        existing_loc_count = 0
        print("📊 LOCATION_CLEAN chưa có, sẽ tạo mới")
    
    # Clear cache để đảm bảo đọc dữ liệu mới nhất
    spark.catalog.clearCache()
    
    # Gọi normalize_locations (sẽ overwrite, nhưng ta sẽ merge lại sau)
    try:
        location_mapping = normalize_locations(spark)
    except Exception as e:
        print(f"❌ Lỗi khi normalize_locations: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        # Fallback: Không có mapping, chỉ dùng dữ liệu hiện có
        location_mapping = {}
        print("⚠️ Sử dụng location_mapping rỗng, giữ nguyên dữ liệu CLEAN hiện có")
    
    # Clear cache lại sau khi normalize
    spark.catalog.clearCache()
    
    # Đọc CLEAN mới sau khi normalize
    try:
        df_loc_clean_new = read_table_from_oracle(spark, "LOCATION_DIMENSION_CLEAN", DB_USER)
        new_loc_count = df_loc_clean_new.count()
        
        # Kiểm tra nếu bảng CLEAN mới rỗng nhưng có dữ liệu cũ
        if new_loc_count == 0 and existing_loc_count > 0:
            print("⚠️ Bảng CLEAN mới rỗng nhưng có dữ liệu cũ. Giữ nguyên dữ liệu cũ...")
            write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
            print(f"✅ Đã giữ nguyên LOCATION_DIMENSION_CLEAN: {existing_loc_count} records")
        
        # Merge: Giữ nguyên CLEAN cũ + CLEAN mới (union và distinct)
        elif df_loc_clean_existing is not None and existing_loc_count > 0:
            df_loc_clean_combined = df_loc_clean_existing.unionByName(df_loc_clean_new, allowMissingColumns=True)
            df_loc_clean_final = df_loc_clean_combined.distinct()
            final_count = df_loc_clean_final.count()
            
            # Đảm bảo có dữ liệu trước khi ghi
            if final_count > 0:
                write_table_to_oracle(df_loc_clean_final, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã cập nhật LOCATION_DIMENSION_CLEAN: {final_count} records (giữ {existing_loc_count} cũ)")
            else:
                print("⚠️ Sau merge không còn dữ liệu! Giữ nguyên dữ liệu cũ...")
                write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên LOCATION_DIMENSION_CLEAN: {existing_loc_count} records")
        else:
            # Kiểm tra nếu bảng CLEAN mới có dữ liệu
            if new_loc_count > 0:
                print(f"✅ Đã tạo LOCATION_DIMENSION_CLEAN: {new_loc_count} records")
            else:
                print("⚠️ Bảng CLEAN mới rỗng! Kiểm tra lại bảng gốc...")
                # Fallback: đọc từ bảng gốc
                try:
                    df_original = read_table_from_oracle(spark, "LOCATION_DIMENSION", DB_USER)
                    original_count = df_original.count()
                    if original_count > 0:
                        print(f"⚠️ Copy {original_count} records từ bảng gốc...")
                        write_table_to_oracle(df_original, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                        print(f"✅ Đã copy từ bảng gốc: {original_count} records")
                    else:
                        print("❌ Bảng gốc cũng trống!")
                except Exception as e2:
                    print(f"❌ Không thể copy từ bảng gốc: {e2}")
    except Exception as e:
        print(f"⚠️ Lỗi khi merge LOCATION_CLEAN: {e}")
        # Fallback: giữ nguyên dữ liệu cũ nếu có
        if df_loc_clean_existing is not None and existing_loc_count > 0:
            try:
                write_table_to_oracle(df_loc_clean_existing, f"{DB_USER}.LOCATION_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên dữ liệu cũ: {existing_loc_count} records")
            except:
                pass
    
    print(f"✅ Location mapping: {len(location_mapping)} mappings")
    
    # B2: GOLD TYPE enrich -> GOLD_TYPE_DIMENSION_CLEAN
    print("\n💎 Bước 2: Enrich GOLD_TYPE_DIMENSION...")
    
    # Đọc dữ liệu CLEAN hiện có TRƯỚC (để giữ lại)
    try:
        df_type_clean_existing = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
        existing_type_count = df_type_clean_existing.count()
        print(f"📊 TYPE_CLEAN hiện có: {existing_type_count} records")
    except:
        df_type_clean_existing = None
        existing_type_count = 0
        print("📊 TYPE_CLEAN chưa có, sẽ tạo mới")
    
    # Clear cache để đảm bảo đọc dữ liệu mới nhất
    spark.catalog.clearCache()
    
    # Gọi các hàm enrich (sẽ overwrite, nhưng ta sẽ merge lại sau)
    try:
        enrich_gold_types(spark)
        normalize_purity_format(spark)
        normalize_category_smart(spark)
    except Exception as e:
        print(f"❌ Lỗi khi enrich/normalize TYPE: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        print("⚠️ Giữ nguyên dữ liệu TYPE_CLEAN hiện có")
    
    # Clear cache lại sau khi gọi các hàm
    spark.catalog.clearCache()
    
    # Đọc CLEAN mới sau khi enrich
    try:
        df_type_clean_new = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION_CLEAN", DB_USER)
        new_type_count = df_type_clean_new.count()
        
        # Kiểm tra nếu bảng CLEAN mới rỗng nhưng có dữ liệu cũ
        if new_type_count == 0 and existing_type_count > 0:
            print("⚠️ Bảng CLEAN mới rỗng nhưng có dữ liệu cũ. Giữ nguyên dữ liệu cũ...")
            write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
            print(f"✅ Đã giữ nguyên GOLD_TYPE_DIMENSION_CLEAN: {existing_type_count} records")
            return location_mapping, {}
        
        # Merge: Giữ nguyên CLEAN cũ + CHỈ THÊM record MỚI (không chỉnh sửa record cũ)
        if df_type_clean_existing is not None and existing_type_count > 0:
            # Lấy danh sách ID đã tồn tại
            existing_ids_df = df_type_clean_existing.select("ID").dropDuplicates()
            # Giữ lại chỉ những record mới có ID CHƯA tồn tại trong bảng cũ
            df_type_clean_new_only = df_type_clean_new.join(
                existing_ids_df,
                on="ID",
                how="left_anti"
            )
            # Union dữ liệu cũ + dữ liệu mới (ID mới)
            df_type_clean_final = df_type_clean_existing.unionByName(
                df_type_clean_new_only,
                allowMissingColumns=True
            )
            final_count = df_type_clean_final.count()
            
            # Đảm bảo có dữ liệu trước khi ghi
            if final_count > 0:
                write_table_to_oracle(df_type_clean_final, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã cập nhật GOLD_TYPE_DIMENSION_CLEAN: {final_count} records (giữ nguyên {existing_type_count} record cũ, thêm {df_type_clean_new_only.count()} record mới)")
            else:
                print("⚠️ Sau merge không còn dữ liệu! Giữ nguyên dữ liệu cũ...")
                write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên GOLD_TYPE_DIMENSION_CLEAN: {existing_type_count} records")
        else:
            # Kiểm tra nếu bảng CLEAN mới có dữ liệu
            if new_type_count > 0:
                print(f"✅ Đã tạo GOLD_TYPE_DIMENSION_CLEAN: {new_type_count} records")
            else:
                print("⚠️ Bảng CLEAN mới rỗng! Kiểm tra lại bảng gốc...")
                # Fallback: đọc từ bảng gốc
                try:
                    df_original = read_table_from_oracle(spark, "GOLD_TYPE_DIMENSION", DB_USER)
                    original_count = df_original.count()
                    if original_count > 0:
                        print(f"⚠️ Copy {original_count} records từ bảng gốc...")
                        write_table_to_oracle(df_original, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                        print(f"✅ Đã copy từ bảng gốc: {original_count} records")
                    else:
                        print("❌ Bảng gốc cũng trống!")
                except Exception as e2:
                    print(f"❌ Không thể copy từ bảng gốc: {e2}")
    except Exception as e:
        print(f"⚠️ Lỗi khi merge TYPE_CLEAN: {e}")
        # Fallback: giữ nguyên dữ liệu cũ nếu có
        if df_type_clean_existing is not None and existing_type_count > 0:
            try:
                write_table_to_oracle(df_type_clean_existing, f"{DB_USER}.GOLD_TYPE_DIMENSION_CLEAN", "overwrite")
                print(f"✅ Đã giữ nguyên dữ liệu cũ: {existing_type_count} records")
            except:
                pass
    
    # (Tuỳ chọn) gộp TYPE tương đồng
    type_mapping = {}
    if merge_types:
        print("\n🔗 Bước 3: Merge duplicate types...")
        try:
            type_mapping = merge_duplicate_types_and_update_fact(spark)
            print(f"✅ Type mapping: {len(type_mapping)} mappings")
        except Exception as e:
            print(f"❌ Lỗi khi merge duplicate types: {e}")
            print(f"   Traceback: {type(e).__name__}: {str(e)}")
            type_mapping = {}
            print("⚠️ Sử dụng type_mapping rỗng")
    else:
        print("\n⏭️  Bước 3: Bỏ qua merge types (dùng --merge-types để bật)")
    
    try:
        normalize_gold_type_and_unit(spark)
    except Exception as e:
        print(f"❌ Lỗi khi normalize_gold_type_and_unit: {e}")
        print(f"   Traceback: {type(e).__name__}: {str(e)}")
        print("⚠️ Bỏ qua bước normalize_gold_type_and_unit")
    
    print("\n✅ Đã clean tất cả dimension tables (giữ nguyên dữ liệu cũ)!")
    print("="*60 + "\n")
    
    return location_mapping, type_mapping

def process_batch(batch_id: int, batch_df: 'DataFrame', 
                 spark: SparkSession, table_name: str,
                 clean_all: bool = False, merge_types: bool = False):
    """
    Xử lý mỗi batch trong streaming.
    Được gọi tự động bởi foreachBatch.
    
    Nếu clean_all=True, sẽ clean tất cả bảng (LOCATION, TYPE, FACT) mỗi khi FACT thay đổi.
    """
    print(f"\n{'='*60}")
    print(f"📦 Batch {batch_id} - {dt.datetime.now()}")
    print(f"{'='*60}")
    
    # Bỏ qua batch_df (không dùng, chỉ là trigger)
    # Đọc dữ liệu mới từ Oracle
    last_ts = get_last_timestamp_from_checkpoint(spark)
    print(f"🔍 Đang kiểm tra dữ liệu mới sau {last_ts}...")
    
    df_new = read_new_data_from_oracle(spark, table_name, last_ts)
    
    if df_new.count() == 0:
        print("ℹ️ Không có dữ liệu mới trong batch này")
        return
    
    print(f"📊 Số lượng records FACT mới: {df_new.count()}")
    
    # Nếu clean_all=True, clean tất cả dimension tables trước
    location_mapping = {}
    type_mapping = {}
    
    if clean_all:
        print("\n🔄 Phát hiện FACT thay đổi, đang clean TẤT CẢ các bảng...")
        print("   (Giữ nguyên dữ liệu CLEAN cũ, chỉ cập nhật/thêm mới)")
        location_mapping, type_mapping = clean_all_dimensions_incremental(spark, merge_types)
    else:
        # Chỉ load mappings hiện có
        location_mapping, type_mapping = load_dimension_mappings(spark)
        print(f"📊 Sử dụng mappings hiện có: Location={len(location_mapping)}, Type={len(type_mapping)}")
    
    # Xử lý dữ liệu FACT mới với mappings
    df_processed = process_new_fact_data(spark, df_new, location_mapping, type_mapping)
    
    if df_processed.count() == 0:
        print("⚠️ Sau xử lý không còn dữ liệu")
        return
    
    # Merge với dữ liệu CLEAN hiện có (CHỈ THÊM, KHÔNG XÓA DỮ LIỆU CŨ) - Logic giống batch file
    try:
        # Đọc bảng CLEAN hiện có
        df_existing = read_table_from_oracle(spark, "GOLD_PRICE_FACT_CLEAN", DB_USER)
        existing_count = df_existing.count()
        print(f"📊 GOLD_PRICE_FACT_CLEAN hiện có: {existing_count} records")
        
        # Union dữ liệu mới với dữ liệu cũ
        df_combined = df_existing.unionByName(df_processed, allowMissingColumns=True)
        combined_count = df_combined.count()
        processed_count = df_processed.count()
        print(f"📊 Sau merge: {combined_count} records (cũ: {existing_count}, mới: {processed_count})")
        
        # Apply cleaning trên dữ liệu đã merge (dedup, handle missing, flag outliers)
        # Logic giống hệt batch file để đảm bảo consistency
        print("🧹 Đang xử lý cleaning trên dữ liệu đã merge...")
        
        # 1. Dedup trên toàn bộ dữ liệu đã merge
        df_combined = df_combined.cache()
        before_dedup = df_combined.count()
        
        # Tạo composite key để dedup (với RECORDED_AT_SAFE để handle null)
        df_combined = df_combined.withColumn(
            "COMBO",
            concat_ws("|", 
                col("SOURCE_ID").cast("string"),
                col("TYPE_ID").cast("string"),
                col("LOCATION_ID").cast("string"),
                col("TIME_ID").cast("string")
            )
        ).withColumn(
            "RECORDED_AT_SAFE",
            coalesce(col(TIMESTAMP_COLUMN), to_timestamp(lit("2000-01-01 00:00:00")))
        )
        
        window_spec = Window.partitionBy("COMBO").orderBy(col("RECORDED_AT_SAFE").desc())
        df_combined = df_combined.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn", "COMBO", "RECORDED_AT_SAFE")
        
        after_dedup = df_combined.count()
        n_dup = before_dedup - after_dedup
        print(f"   ✅ Đã loại bỏ {n_dup} bản ghi trùng")
        
        # 2. Handle missing values (chỉ loại bỏ record thiếu critical fields)
        before_missing = df_combined.count()
        df_combined = df_combined.filter(
            col("BUY_PRICE").isNotNull() & 
            col("SELL_PRICE").isNotNull() & 
            col(TIMESTAMP_COLUMN).isNotNull()
        )
        after_missing = df_combined.count()
        n_missing = before_missing - after_missing
        print(f"   ✅ Đã loại bỏ {n_missing} bản ghi thiếu giá hoặc thời gian")
        
        # 3. Flag outliers (không xóa, chỉ flag) - Logic giống batch file
        from pyspark.sql.functions import percentile_approx
        from decimal import Decimal
        
        def to_float(val):
            if val is None:
                return None
            if isinstance(val, Decimal):
                return float(val)
            return float(val)
        
        try:
            buy_q1_val = df_combined.select(percentile_approx("BUY_PRICE", 0.25).alias("q1")).first()[0]
            buy_q3_val = df_combined.select(percentile_approx("BUY_PRICE", 0.75).alias("q3")).first()[0]
            buy_q1 = to_float(buy_q1_val)
            buy_q3 = to_float(buy_q3_val)
            buy_iqr = buy_q3 - buy_q1
            buy_lower = buy_q1 - 1.5 * buy_iqr
            buy_upper = buy_q3 + 1.5 * buy_iqr
            
            sell_q1_val = df_combined.select(percentile_approx("SELL_PRICE", 0.25).alias("q1")).first()[0]
            sell_q3_val = df_combined.select(percentile_approx("SELL_PRICE", 0.75).alias("q3")).first()[0]
            sell_q1 = to_float(sell_q1_val)
            sell_q3 = to_float(sell_q3_val)
            sell_iqr = sell_q3 - sell_q1
            sell_lower = sell_q1 - 1.5 * sell_iqr
            sell_upper = sell_q3 + 1.5 * sell_iqr
            
            df_combined = df_combined.withColumn(
                "IS_DELETED",
                when(
                    (col("BUY_PRICE") < lit(buy_lower)) | (col("BUY_PRICE") > lit(buy_upper)) |
                    (col("SELL_PRICE") < lit(sell_lower)) | (col("SELL_PRICE") > lit(sell_upper)),
                    lit(1)
                ).otherwise(lit(0))
            )
            
            n_outliers = df_combined.filter(col("IS_DELETED") == 1).count()
            print(f"   ✅ Đã flag {n_outliers} bản ghi outlier (IS_DELETED=1)")
        except Exception as e:
            print(f"   ⚠️ Không thể flag outliers: {e}. Giữ nguyên dữ liệu.")
            if "IS_DELETED" not in df_combined.columns:
                df_combined = df_combined.withColumn("IS_DELETED", lit(0))
        
        # Đảm bảo có cột IS_DELETE (nếu cần)
        if "IS_DELETE" not in df_combined.columns:
            df_combined = df_combined.withColumn("IS_DELETE", col("IS_DELETED"))
        
        # Ghi lại bảng CLEAN với dữ liệu đã merge và đã clean
        final_count = df_combined.count()
        write_table_to_oracle(df_combined, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
        print(f"✅ Đã merge và clean: {final_count} records (thêm {processed_count} mới, giữ {existing_count} cũ)")
        
        # Cập nhật checkpoint với timestamp mới nhất
        max_ts = df_processed.agg(spark_max(col(TIMESTAMP_COLUMN))).first()[0]
        if max_ts:
            update_checkpoint(spark, max_ts)
            print(f"✅ Đã cập nhật checkpoint: {max_ts}")
    
    except Exception as e:
        # Nếu bảng CLEAN chưa có, ghi dữ liệu mới (chỉ lần đầu)
        print(f"⚠️ Bảng CLEAN chưa có hoặc lỗi: {e}. Ghi dữ liệu mới...")
        # Apply basic cleaning trước khi ghi
        df_processed = df_processed.filter(
            col("BUY_PRICE").isNotNull() & 
            col("SELL_PRICE").isNotNull() & 
            col(TIMESTAMP_COLUMN).isNotNull()
        )
        if "IS_DELETED" not in df_processed.columns:
            df_processed = df_processed.withColumn("IS_DELETED", lit(0))
        if "IS_DELETE" not in df_processed.columns:
            df_processed = df_processed.withColumn("IS_DELETE", lit(0))
        write_table_to_oracle(df_processed, f"{DB_USER}.GOLD_PRICE_FACT_CLEAN", "overwrite")
        print(f"✅ Đã ghi {df_processed.count()} records vào GOLD_PRICE_FACT_CLEAN (lần đầu)")

def create_oracle_polling_stream(spark: SparkSession, table_name: str,
                                trigger_interval: str = STREAMING_TRIGGER_INTERVAL,
                                clean_all: bool = False,
                                merge_types: bool = False):
    """
    Tạo Spark Structured Streaming query để polling Oracle.
    
    Cách hoạt động:
    1. Dùng rate source để tạo trigger (emit 1 row mỗi interval)
    2. Dùng foreachBatch để polling Oracle mỗi interval
    3. Nếu clean_all=True, sẽ clean tất cả bảng mỗi khi FACT thay đổi
    4. Spark tự động quản lý checkpoint và recovery
    """
    
    # Tạo rate source - emit 1 row mỗi interval để trigger foreachBatch
    # Rate source là built-in streaming source của Spark
    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .option("numPartitions", 1) \
        .load()
    
    # Chỉ lấy timestamp column để làm trigger
    trigger_df = trigger_df.select(col("timestamp").alias("trigger_time"))
    
    # Tạo streaming query với foreachBatch
    def foreach_batch_wrapper(batch_id, batch_df):
        # Bỏ qua batch_df (chỉ là trigger)
        # Gọi process_batch để polling Oracle và clean nếu cần
        process_batch(batch_id, batch_df, spark, table_name, clean_all, merge_types)
    
    # Tạo streaming query
    query = trigger_df.writeStream \
        .foreachBatch(foreach_batch_wrapper) \
        .outputMode("update") \
        .trigger(processingTime=trigger_interval) \
        .option("checkpointLocation", f"{STREAMING_CHECKPOINT_DIR}/oracle_polling") \
        .start()
    
    return query

# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming với Oracle polling")
    parser.add_argument("--interval", type=str, default=STREAMING_TRIGGER_INTERVAL,
                       help="Trigger interval (ví dụ: '30 seconds', '1 minute')")
    parser.add_argument("--table", type=str, default="GOLD_PRICE_FACT",
                       help="Tên bảng Oracle để monitor (GOLD_PRICE_FACT, LOCATION_DIMENSION, GOLD_TYPE_DIMENSION)")
    parser.add_argument("--clean-all", action="store_true",
                       help="Khi FACT thay đổi, tự động clean TẤT CẢ các bảng (LOCATION, TYPE, FACT)")
    parser.add_argument("--merge-types", action="store_true",
                       help="Gộp TYPE tương đồng khi clean (chỉ dùng với --clean-all)")
    
    args = parser.parse_args()
    
    # Tạo checkpoint directory
    os.makedirs(STREAMING_CHECKPOINT_DIR, exist_ok=True)
    
    spark = create_spark_session()
    
    print("\n" + "="*60)
    print("🚀 SPARK STRUCTURED STREAMING - ORACLE POLLING")
    print("="*60)
    print(f"📊 Table: {args.table}")
    print(f"⏱️  Trigger Interval: {args.interval}")
    print(f"📁 Checkpoint: {STREAMING_CHECKPOINT_DIR}")
    if args.clean_all:
        print(f"🔄 Mode: Clean ALL tables khi FACT thay đổi")
        print(f"   ✅ LOCATION_DIMENSION → LOCATION_DIMENSION_CLEAN")
        print(f"   ✅ GOLD_TYPE_DIMENSION → GOLD_TYPE_DIMENSION_CLEAN")
        print(f"   ✅ GOLD_PRICE_FACT → GOLD_PRICE_FACT_CLEAN")
        if args.merge_types:
            print(f"   ✅ Merge duplicate types: ON")
    else:
        print(f"🔄 Mode: Streaming FACT only (chỉ xử lý FACT)")
    print("="*60 + "\n")
    
    # Kiểm tra batch functions có sẵn không
    if args.clean_all and not BATCH_FUNCTIONS_AVAILABLE:
        print("❌ Lỗi: Không thể import batch functions để clean dimensions!")
        print("   Vui lòng đảm bảo các dependencies đã được cài:")
        print("   pip install pandas numpy scikit-learn fuzzywuzzy python-Levenshtein")
        print("\n   Hoặc chạy không có --clean-all để chỉ xử lý FACT")
        sys.exit(1)
    
    # Chỉ streaming FACT table
    if args.table != "GOLD_PRICE_FACT":
        print(f"⚠️ Lưu ý: Streaming chỉ hỗ trợ GOLD_PRICE_FACT")
        print(f"   Đang chuyển sang GOLD_PRICE_FACT...\n")
        args.table = "GOLD_PRICE_FACT"
    
    # Khởi động streaming query
    query = create_oracle_polling_stream(
        spark, 
        args.table, 
        args.interval,
        args.clean_all,
        args.merge_types
    )
    
    print(f"\n✅ Streaming query đã khởi động!")
    print(f"📊 Query ID: {query.id}")
    print(f"📊 Status: {query.status}")
    print(f"📊 Spark UI: http://localhost:4040")
    print(f"\n🔄 Đang chạy... Nhấn Ctrl+C để dừng\n")
    
    try:
        # Chờ streaming query chạy
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️ Đang dừng streaming query...")
        query.stop()
        print("✅ Đã dừng")
    
    spark.stop()

if __name__ == "__main__":
    main()

