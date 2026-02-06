from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, from_json, current_timestamp, explode, regexp_replace, when, decode, udf, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from datetime import datetime
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

import re
import json
import os
import logging

# ===========================
# ⚙️ CẤU HÌNH ORACLE DATABASE
# ===========================
oracle_url = "jdbc:oracle:thin:@//136.110.60.196/XEPDB1"
oracle_properties = {
    "user": "CLOUD",
    "password": "cloud123",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
# Ví dụ:
# oracle_url = "jdbc:oracle:thin:@//localhost:1521/XEPDB1"
# oracle_properties = {"user": "CLOUD", "password": "admin123", "driver": "oracle.jdbc.driver.OracleDriver"}

# ===========================
# 🚀 KHỞI TẠO SPARK SESSION
# ===========================
spark = SparkSession.builder \
    .appName("KafkaGoldPriceProcessor") \
    .config("spark.master", "local[*]") \
    .config("spark.jars", "C:\\Users\\namhh\\ojdbc11.jar") \
    .config("spark.driver.extraClassPath", "C:\\Users\\namhh\\ojdbc11.jar") \
    .config("spark.executor.extraClassPath", "C:\\Users\\namhh\\ojdbc11.jar") \
    .config("spark.sql.streaming.kafka.maxOffsetsPerTrigger", "1000") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")
# Suppress KAFKA-1894 warnings about UninterruptibleThread
logging.getLogger("org.apache.spark.streaming.kafka").setLevel("ERROR")
logging.getLogger("org.apache.kafka.clients").setLevel("ERROR")

# ===========================
# 🧩 SCHEMA JSON KAFKA
# ===========================
gold_item_schema = StructType([
    StructField("loai", StringType(), True),
    StructField("cong_ty", StringType(), True),
    StructField("mua", StringType(), True),
    StructField("ban", StringType(), True)
])
gold_array_schema = ArrayType(gold_item_schema)

# ===========================
# 🧠 HÀM UDF PARSING
# ===========================
@udf(returnType=StructType([
    StructField("location", StringType(), True),
    StructField("category", StringType(), True),
    StructField("type_name", StringType(), True),
    StructField("purity", StringType(), True)
]))
def parse_gold_info(loai):
    if not loai:
        return (None, None, None, None)
    locations = ["Hồ Chí Minh", "Hà Nội", "Miền Bắc", "Hạ Long", "Hải Phòng",
                 "Miền Trung", "Huế", "Quảng Ngãi", "Nha Trang", "Biên Hòa",
                 "Miền Tây", "Bạc Liêu", "Cà Mau"]
    location = None
    category = None
    type_name = loai
    purity = None
    for loc in locations:
        if loc in loai:
            location = loc
            type_name = loai.replace(loc, "").strip()
            break
    purity_patterns = [
        r'(\d{1,3}[.,]\d{1,2})%',  # 99.9%, 99,99%
        r'(\d{3,4}\.\d)',          # 999.9
        r'(\d{2,3})%'              # 99%
    ]
    for pattern in purity_patterns:
        match = re.search(pattern, type_name)
        if match:
            purity = match.group(1).replace(',', '.')
            type_name = re.sub(pattern, '', type_name).strip()
            break
    loai_lower = loai.lower()
    if "vàng sjc" in loai_lower or "vàng miếng" in loai_lower:
        category = "gold_bar"
    elif "trang sức" in loai_lower or "nữ trang" in loai_lower:
        category = "jewelry"
    elif "nhẫn" in loai_lower:
        category = "ring"
    elif "quà tặng" in loai_lower or "thần tài" in loai_lower:
        category = "gift"
    else:
        if re.search(r'\d+L|1KG|\d+ chỉ', loai):
            category = "gold_bar"
        else:
            category = "other"
    type_name = re.sub(r'\s+', ' ', type_name).strip()
    return (location, category, type_name, purity)

@udf(returnType=StructType([
    StructField("city", StringType(), True),
    StructField("region", StringType(), True)
]))
def map_location_to_city_region(location):
    mapping = {
        "Hồ Chí Minh": ("Hồ Chí Minh", "Miền Nam"),
        "Hà Nội": ("Hà Nội", "Miền Bắc"),
        "Hạ Long": ("Hạ Long", "Miền Bắc"),
        "Hải Phòng": ("Hải Phòng", "Miền Bắc"),
        "Huế": ("Huế", "Miền Trung"),
        "Quảng Ngãi": ("Quảng Ngãi", "Miền Trung"),
        "Nha Trang": ("Nha Trang", "Miền Trung"),
        "Biên Hòa": ("Biên Hòa", "Miền Nam"),
        "Bạc Liêu": ("Bạc Liêu", "Miền Tây"),
        "Cà Mau": ("Cà Mau", "Miền Tây"),
        "Miền Bắc": (None, "Miền Bắc"),
        "Miền Trung": (None, "Miền Trung"),
        "Miền Tây": (None, "Miền Tây")
    }
    return mapping.get(location, (None, None))

@udf(returnType=StringType())
def normalize_purity(purity_str):
    if not purity_str:
        return None
    purity_str = purity_str.strip()
    if re.match(r'^(999\.9|99\.99|9999)$', purity_str): return "99.99"
    if re.match(r'^(99\.9|999)$', purity_str): return "99.9"
    if re.match(r'^(99\.0|990)$', purity_str): return "99.0"
    return purity_str

# ===========================
# 🔄 ĐỌC TỪ KAFKA
# ===========================
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vietnam-gold-price-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = kafka_df.select(
    col("value").cast("string").alias("raw_value"),
    decode(col("value"), "Cp1258").alias("json_payload"), 
    col("timestamp"),
    col("offset"),
    col("partition")
)

parsed_df = json_df \
    .withColumn("data", from_json(col("json_payload"), gold_array_schema)) \
    .filter(col("data").isNotNull()) \
    .withColumn("item", explode(col("data"))) \
    .select(
        col("item.loai").alias("Loai"),
        col("item.cong_ty").alias("CongTy"),
        col("item.mua").alias("mua_raw"),
        col("item.ban").alias("ban_raw"),
        col("timestamp").alias("KafkaTimestamp"),
        col("offset").alias("KafkaOffset"),
        col("partition").alias("KafkaPartition")
    )

parsed_df = parsed_df \
    .withColumn("parsed_info", parse_gold_info(col("Loai"))) \
    .withColumn("location_info", map_location_to_city_region(col("parsed_info.location"))) \
    .select(
        col("Loai"),
        col("CongTy"),
        col("mua_raw"),
        col("ban_raw"),
        date_format(col("KafkaTimestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("KafkaTimestamp"),
        col("KafkaOffset"),
        col("KafkaPartition"),
        col("parsed_info.location").alias("Location"),
        col("location_info.city").alias("City"),
        col("location_info.region").alias("Region"),
        col("parsed_info.category").alias("Category"),
        col("parsed_info.type_name").alias("TypeName"),
        normalize_purity(col("parsed_info.purity")).alias("Purity")
    ) \
    .withColumn("GiaMua", when(col("mua_raw") == "--", None)
                .otherwise(regexp_replace(col("mua_raw"), "[^0-9\\.]", ""))
                .cast(DoubleType())) \
    .withColumn("GiaBan", when(col("ban_raw") == "--", None)
                .otherwise(regexp_replace(col("ban_raw"), "[^0-9\\.]", ""))
                .cast(DoubleType())) \
    .withColumn("ProcessTime", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm")) \

# ===========================
# 💾 GHI XUỐNG ORACLE
# ===========================
def upsert_dimensions_and_fact(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # ====== Load dimensions ======
    source_dim = spark.read.jdbc(oracle_url, "CLOUD.SOURCE_DIMENSION", properties=oracle_properties)
    type_dim = spark.read.jdbc(oracle_url, "CLOUD.GOLD_TYPE_DIMENSION", properties=oracle_properties)
    loc_dim = spark.read.jdbc(oracle_url, "CLOUD.LOCATION_DIMENSION", properties=oracle_properties)
    time_dim = spark.read.jdbc(oracle_url, "CLOUD.TIME_DIMENSION", properties=oracle_properties)

    # Chuẩn hoá dữ liệu incoming
    df = (
        batch_df
        .withColumnRenamed("CongTy", "SOURCE_NAME")
        .withColumn("TypeName", F.when(F.col("TypeName").isNull(), F.lit("Unknown")).otherwise(F.trim(F.col("TypeName"))))
        .withColumn("Purity", F.when(F.col("Purity").isNull(), F.lit("Unknown")).otherwise(F.trim(F.col("Purity"))))
        .withColumn("Category", F.when(F.col("Category").isNull(), F.lit("Unknown")).otherwise(F.trim(F.col("Category"))))
        .withColumn("City", F.when(F.col("City").isNull(), F.lit("Unknown")).otherwise(F.trim(F.col("City"))))
        .withColumn("Region", F.when(F.col("Region").isNull(), F.lit("Unknown")).otherwise(F.trim(F.col("Region"))))
    )

    # ----------------------------
    # SOURCE_DIMENSION
    # ----------------------------
    new_sources = df.select("SOURCE_NAME").distinct() \
        .join(source_dim, "SOURCE_NAME", "left_anti")
    if new_sources.limit(1).count() > 0:
        new_sources.write.jdbc(oracle_url, "CLOUD.SOURCE_DIMENSION", "append", properties=oracle_properties)
        source_dim = spark.read.jdbc(oracle_url, "CLOUD.SOURCE_DIMENSION", properties=oracle_properties)

    # ----------------------------
    # GOLD_TYPE_DIMENSION (với BRAND = SOURCE_NAME)
    # ----------------------------

    # chuẩn bị dữ liệu nguồn
    df_types = df.select(
        F.trim(F.col("TypeName")).alias("TypeName"),
        F.trim(F.col("Purity")).alias("Purity"),
        F.trim(F.col("Category")).alias("Category"),
        F.trim(F.col("SOURCE_NAME")).alias("Brand")
    ).distinct().alias("src")

    # alias cho dimension table
    type_dim_alias = type_dim.alias("dim")

    # điều kiện join: so sánh các cột chính xác
    join_cond = (
        (F.col("src.TypeName") == F.col("dim.TYPE_NAME")) &
        (F.col("src.Purity") == F.col("dim.PURITY")) &
        (F.col("src.Category") == F.col("dim.CATEGORY")) &
        (F.col("src.Brand") == F.col("dim.BRAND"))
    )

    # chỉ lấy các hàng chưa có trong dimension
    new_types = df_types.join(type_dim_alias, join_cond, "left_anti")

    # nếu có dòng mới -> insert vào dimension
    if new_types.limit(1).count() > 0:
        (
            new_types.select(
                F.col("src.TypeName").alias("TYPE_NAME"),
                F.col("src.Purity").alias("PURITY"),
                F.col("src.Category").alias("CATEGORY"),
                F.col("src.Brand").alias("BRAND")
            )
            .write.jdbc(
                oracle_url,
                "CLOUD.GOLD_TYPE_DIMENSION",
                "append",
                properties=oracle_properties
            )
        )

    # reload lại dimension sau khi insert
    type_dim = spark.read.jdbc(oracle_url, "CLOUD.GOLD_TYPE_DIMENSION", properties=oracle_properties)


    # ----------------------------
    # LOCATION_DIMENSION
    # ----------------------------
    df_loc = df.select("City", "Region").distinct().alias("src")
    loc_dim_alias = loc_dim.alias("dim")

    join_cond = (
        (F.col("src.City") == F.col("dim.CITY")) &
        (F.col("src.Region") == F.col("dim.REGION"))
    )

    new_locs = df_loc.join(loc_dim_alias, join_cond, "left_anti")

    if new_locs.limit(1).count() > 0:
        (
            new_locs
            .select(
                F.col("src.City").alias("CITY"),
                F.col("src.Region").alias("REGION")
            )
            .write.jdbc(oracle_url, "CLOUD.LOCATION_DIMENSION", "append", properties=oracle_properties)
        )
        loc_dim = spark.read.jdbc(oracle_url, "CLOUD.LOCATION_DIMENSION", properties=oracle_properties)
    # ----------------------------
    # ===== TIME_DIMENSION (fix: keep timestamp & extract hour) =====

    # parse KafkaTimestamp into timestamp (specify format if Kafka sends ISO8601)
    # nếu KafkaTimestamp đã là timestamp type thì bỏ to_timestamp(...) đi
    df_time = df.select(
        F.to_timestamp(F.col("KafkaTimestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("ts")  # adjust format nếu cần
    ).distinct().alias("src")

    # prepare existing time_dim
    time_dim = spark.read.jdbc(oracle_url, "CLOUD.TIME_DIMENSION", properties=oracle_properties)
    time_dim_alias = time_dim.alias("dim")

    # compare on DATE (or on exact timestamp; here we compare date+hour to be safe)
    # create a comparable column: cast ts -> date (or keep timestamp)
    df_time_cmp = df_time.select(
        F.col("ts").alias("DATE_TIME"),
        F.year("ts").alias("YEAR"),
        F.month("ts").alias("MONTH"),
        F.dayofmonth("ts").alias("DAY"),
        F.hour("ts").alias("HOUR")
    ).alias("src")

    # If your TIME_DIMENSION.DATE_TIME is TIMESTAMP, match exactly; if it's DATE, match on date only.
    # Here try matching on DATE_TIME truncated to date and hour to avoid duplicates
    join_cond = (
        (F.to_timestamp(F.col("src.DATE_TIME")) == F.to_timestamp(F.col("dim.DATE_TIME"))) |
        ((F.to_date(F.col("src.DATE_TIME")) == F.to_date(F.col("dim.DATE_TIME"))) &
        (F.hour(F.col("src.DATE_TIME")) == F.col("dim.HOUR")))
    )

    new_times = df_time_cmp.join(time_dim_alias, join_cond, "left_anti")

    if new_times.limit(1).count() > 0:
        to_insert_times = new_times.select(
            F.col("src.DATE_TIME").alias("DATE_TIME"),
            F.col("src.YEAR").alias("YEAR"),
            F.col("src.MONTH").alias("MONTH"),
            F.col("src.DAY").alias("DAY"),
            F.col("src.HOUR").cast("int").alias("HOUR")
        )
        # debug:
        # to_insert_times.printSchema(); to_insert_times.show(truncate=False)
        to_insert_times.write.jdbc(oracle_url, "CLOUD.TIME_DIMENSION", "append", properties=oracle_properties)

    # reload
    time_dim = spark.read.jdbc(oracle_url, "CLOUD.TIME_DIMENSION", properties=oracle_properties)


    # ----------------------------
    # FACT TABLE
    # ----------------------------
    fact_df = (
        df
        .join(source_dim, df.SOURCE_NAME == source_dim.SOURCE_NAME, "left")
        .join(type_dim,
              (df.TypeName == type_dim.TYPE_NAME) &
              (df.Purity == type_dim.PURITY) &
              (df.Category == type_dim.CATEGORY),
              "left")
        .join(loc_dim,
              (df.City == loc_dim.CITY) &
              (df.Region == loc_dim.REGION),
              "left")
        .join(time_dim,
              F.to_date(df.KafkaTimestamp) == F.to_date(time_dim.DATE_TIME),
              "left")
        .select(
            source_dim.ID.alias("SOURCE_ID"),
            type_dim.ID.alias("TYPE_ID"),
            loc_dim.ID.alias("LOCATION_ID"),
            time_dim.ID.alias("TIME_ID"),
            df.GiaMua.alias("BUY_PRICE"),
            df.GiaBan.alias("SELL_PRICE"),
            F.lit("VNĐ/Lượng").alias("UNIT"),
            F.to_timestamp(df.KafkaTimestamp).alias("RECORDED_AT"),
            F.lit("hainam").alias("RECORDED_BY")
        )
    )

    # # Kiểm tra NULL IDs trước khi ghi vào GOLD_PRICE_FACT
    # missing = fact_df.filter(
    #     F.col("SOURCE_ID").isNull() |
    #     F.col("TYPE_ID").isNull() |
    #     F.col("LOCATION_ID").isNull() |
    #     F.col("TIME_ID").isNull()
    # )

    # if missing.limit(1).count() > 0:
    #     # Lưu mẫu thiếu vào file UTF-8 để tránh lỗi encoding (Windows console cp1252 không thể in ký tự Unicode)
    #     rows = [r.asDict() for r in missing.collect()]
    #     # Chuyển các giá trị không serialize được (datetime, Decimal, ...) sang dạng chuỗi/float an toàn
    #     def sanitize(obj):
    #         if isinstance(obj, dict):
    #             return {k: sanitize(v) for k, v in obj.items()}
    #         if isinstance(obj, list):
    #             return [sanitize(v) for v in obj]
    #         if isinstance(obj, datetime):
    #             return obj.isoformat()
    #         try:
    #             import decimal
    #             if isinstance(obj, decimal.Decimal):
    #                 return float(obj)
    #         except Exception:
    #             pass
    #         if isinstance(obj, (str, int, float, bool)) or obj is None:
    #             return obj
    #         return str(obj)

    #     sanitized = [sanitize(r) for r in rows]
    #     # limit to at most 1000 rows to avoid OOM on driver when batch huge
    #     to_save = sanitized if len(sanitized) <= 1000 else sanitized[:1000]
    #     os.makedirs(r"C:\tmp\spark_errors", exist_ok=True)
    #     file_path = os.path.join(r"C:\tmp\spark_errors", f"missing_ids_batch_{batch_id}.json")
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(to_save, f, ensure_ascii=False, indent=2)
    #     # Ghi log ngắn (ASCII-only) và dừng để không ghi fact không đầy đủ
    #     truncated_msg = " (truncated to 1000 rows)" if len(sanitized) > 1000 else ""
    #     print(f"Missing dimension IDs detected: {len(sanitized)} rows saved to {file_path}{truncated_msg}")
    #     raise Exception(f"Missing dimension IDs detected — aborting fact write; saved to {file_path}{truncated_msg}")

    fact_df.write.jdbc(oracle_url, "CLOUD.GOLD_PRICE_FACT", "append", properties=oracle_properties)

# ===========================
# ▶️ KHỞI CHẠY STREAMING
# ===========================
query = parsed_df \
    .writeStream \
    .foreachBatch(upsert_dimensions_and_fact) \
    .outputMode("append") \
    .option("checkpointLocation", "C:\\tmp\\spark_checkpoints\\gold_price_oracle") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
