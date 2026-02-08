package main.spark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.spark.module.entity.*;
import main.spark.repository.GoldPriceFactRepository;
import main.spark.repository.LocationDimensionRepository;
import main.spark.repository.SourceDimensionRepository;
import main.spark.repository.TimeDimensionRepository;
import main.spark.util.LevenshteinUtil;
import main.spark.util.LocationMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class SparkJobService {

    private final SparkSession spark;
    private final GoldPriceFactRepository factRepository;
    private final SourceDimensionRepository sourceDimensionRepository;
    // private final GoldTypeDimensionRepository goldTypeDimensionRepository;
    private final LocationDimensionRepository locationDimensionRepository;
    private final TimeDimensionRepository timeDimensionRepository;
    private final GoldTypeService goldTypeService;

    @Value("${spark.hdfs-uri}")
    private String hdfsUri;
    @Value("${spark.sjc-path}")
    private String sjcPath;
    @Value("${spark.pnj-path}")
    private String pnjPath;

    private String fullSjcPath;
    private String fullPnjPath;
    final DateTimeFormatter DF = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @PostConstruct
    public void init() {
        this.fullSjcPath = hdfsUri + sjcPath;
        this.fullPnjPath = hdfsUri + pnjPath;
    }

    private static final int BATCH_SIZE = 500;

    public void syncSJC() {
        log.info("🚀 Bắt đầu đọc dữ liệu từ HDFS: {}", fullSjcPath);

        // 1️⃣ Đọc file JSON
        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json(fullSjcPath);

        log.info("✅ Schema ban đầu:");
        df.printSchema();

        // 2️⃣ Làm sạch dữ liệu
        Dataset<Row> cleaned = df
                .withColumn("buy_value", regexp_replace(col("buy"), ",", "").cast("double"))
                .withColumn("sell_value", regexp_replace(col("sell"), ",", "").cast("double"))
                .withColumn("branch_name", col("payload.BranchName"))
                .withColumn("type_name", col("payload.TypeName"))
                .select("date", "type", "buy_value", "sell_value", "branch_name", "type_name");

        long totalRows = cleaned.count();
        log.info("✅ Đã làm sạch dữ liệu, tổng số dòng: {}", totalRows);

        // 3️⃣ Duyệt tuần tự từng dòng và lưu batch
        Iterator<Row> iter = cleaned.toLocalIterator();
        List<GoldPriceFact> buffer = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger();

        while (iter.hasNext()) {
            Row row = iter.next();
            GoldPriceFact fact = convertRowToEntity(row);
            if (fact != null) {
                buffer.add(fact);
                counter.incrementAndGet();
            }

            if (buffer.size() >= BATCH_SIZE) {
                saveBatch(buffer, counter.get());
            }
        }

        // 4️⃣ Lưu nốt phần dư
        if (!buffer.isEmpty()) {
            log.info("buffer size: {}", buffer.size());
            saveBatch(buffer, counter.get());
        }

        log.info("💾 Hoàn tất đồng bộ, tổng cộng lưu {} bản ghi.", counter.get());
    }

    private void saveBatch(List<GoldPriceFact> buffer, int counter) {
        try {
            factRepository.saveAll(buffer);
            factRepository.flush();
            log.info("✅ Đã lưu {} bản ghi (tổng cộng).", counter);
        } catch (Exception e) {
            log.error("❌ Lỗi khi lưu batch: {}", e.getMessage());
        } finally {
            buffer.clear();
        }
    }

    private GoldPriceFact convertRowToEntity(Row row) {
        try {
            GoldPriceFact fact = new GoldPriceFact();

            Double buyValue = row.getAs("buy_value");
            Double sellValue = row.getAs("sell_value");
            fact.setBuyPrice(buyValue * 1000);
            fact.setSellPrice(sellValue * 1000);
            fact.setUnit("VNĐ/Lượng");

            // 🔹 SourceDimension
            SourceDimension src = sourceDimensionRepository.findBySourceName("SJC")
                    .orElseGet(() -> {
                        SourceDimension s = new SourceDimension();
                        s.setSourceName("SJC");
                        s.setSourceUrl("https://sjc.com.vn");
                        s.setDescription("Giá vàng SJC chính thức");
                        return s;
                    });
            src.setSourceUrl("https://sjc.com.vn");
            src.setDescription("Giá vàng SJC chính thức");
            sourceDimensionRepository.save(src);
            fact.setSourceDimension(src);

            // 🔹 GoldTypeDimension
            String typeName = row.getAs("type_name");
            GoldTypeDimension type = goldTypeService.getOrCreateGoldType(typeName);
            fact.setGoldTypeDimension(type);

            // 🔹 LocationDimension
            String branch = row.getAs("branch_name");
            if (branch == null)
                branch = "";

            branch = branch.trim()
                    .replaceAll("\\s+", " ")
                    .toLowerCase();

            String city = "Unknown";
            String region = "Unknown";

            // Tìm trong map các tỉnh/thành
            for (Map.Entry<String, String> e : LocationMap.regionMap.entrySet()) {
                if (branch.contains(e.getKey())) {
                    city = LocationMap.capitalizeWords(e.getKey());
                    region = e.getValue();
                    break;
                }
            }

            final String finalCity = city;
            final String finalRegion = region;

            LocationDimension existingLoc;
            if (finalCity != null) {
                existingLoc = locationDimensionRepository.findByCityIsAndRegionIs(finalCity, finalRegion)
                        .orElse(new LocationDimension());
            } else {
                existingLoc = new LocationDimension();
            }

            existingLoc.setCity(finalCity == null ? "Unknown" : finalCity);
            existingLoc.setRegion(finalRegion);
            locationDimensionRepository.save(existingLoc);
            fact.setLocationDimension(existingLoc);

            // 🔹 TimeDimension
            String dateStr = row.getAs("date"); // ví dụ "27/10/2025"
            if (dateStr == null || dateStr.isBlank()) {
                log.warn("⚠️ Bỏ qua bản ghi vì thiếu field 'date'");
                return null;
            }

            LocalDate d = LocalDate.parse(dateStr, DF);
            Integer day = d.getDayOfMonth();
            Integer month = d.getMonthValue();
            Integer year = d.getYear();

            TimeDimension time = timeDimensionRepository.findByDayIsAndMonthIsAndYearIsAndHourIs(day, month, year, 12)
                    .orElseGet(() -> {
                        TimeDimension t = new TimeDimension();
                        t.setDateTime(d);
                        t.setDay(day);
                        t.setMonth(month);
                        t.setYear(year);
                        t.setHour(12);
                        return timeDimensionRepository.save(t);
                    });

            fact.setTimeDimension(time);

            return fact;

        } catch (Exception e) {
            log.warn("⚠️ Lỗi parse record: {}", e.getMessage());
            return null;
        }
    }

    public void syncNestedPrices() {
        log.info("🚀 Đọc JSON nested: {}", fullPnjPath);

        // 1) Đọc file: là 1 mảng JSON duy nhất -> cần multiline=true

        Dataset<Row> raw = spark.read()
                .option("multiline", "true")
                .json(fullPnjPath);

        // 🔹 Bỏ các record không có location hợp lệ
        Dataset<Row> filtered = raw.filter("data.locations IS NOT NULL AND size(data.locations) > 0");

        // 🔹 Flatten
        Dataset<Row> flat = filtered
                .withColumn("date_raw", col("date"))
                .withColumn("location", explode(col("data.locations")))
                .filter("location.gold_type IS NOT NULL AND size(location.gold_type) > 0")
                .withColumn("gold", explode(col("location.gold_type")))
                .filter("gold.data IS NOT NULL AND size(gold.data) > 0")
                .withColumn("point", explode(col("gold.data")))
                .select(
                        col("date_raw"),
                        col("location.name").alias("branch_name"),
                        col("gold.name").alias("type_name"),
                        col("point.gia_mua").alias("buy_raw"),
                        col("point.gia_ban").alias("sell_raw"),
                        col("point.updated_at").alias("updated_at"));

        long total = flat.count();
        log.info("✅ Flatten xong, tổng dòng: {}", total);

        // 3) Duyệt streaming + lưu theo batch
        final int BATCH = 500;
        Iterator<Row> iter = flat.toLocalIterator();
        List<GoldPriceFact> buffer = new ArrayList<>(BATCH);
        int counter = 0;

        while (iter.hasNext()) {
            Row r = iter.next();
            GoldPriceFact fact = convertNestedRowToEntity(r);
            if (fact != null) {
                buffer.add(fact);
                counter++;
            }
            if (buffer.size() >= BATCH) {
                saveBatch(buffer, counter);
            }
        }
        if (!buffer.isEmpty()) {
            saveBatch(buffer, counter);
        }

        log.info("🏁 Hoàn tất syncNestedPrices, đã lưu {} bản ghi.", counter);
    }

    private GoldPriceFact convertNestedRowToEntity(Row row) {
        try {
            String buyStr = row.getAs("buy_raw"); // ví dụ "66.100" hoặc "52.650"
            String sellStr = row.getAs("sell_raw");
            String branch = row.getAs("branch_name"); // "TPHCM", "Miền Tây", "Hà Nội", ...
            String type = row.getAs("type_name"); // "PNJ", "SJC", ...
            String dateRaw = row.getAs("date_raw"); // "yyyyMMdd" -> "20221001"
            String updated = row.getAs("updated_at"); // "dd/MM/yyyy HH:mm:ss"
            String typeRaw = (String) Optional.ofNullable(row.getAs("type_name")).orElse("");

            if (dateRaw == null || buyStr == null || sellStr == null)
                return null;

            // Chuẩn hoá số: "66.400" hoặc "26,620" -> về đơn vị nghìn, rồi *1000 => VNĐ
            double buyVnd = parsePriceVnd(buyStr);
            double sellVnd = parsePriceVnd(sellStr);

            GoldPriceFact fact = new GoldPriceFact();
            fact.setBuyPrice(buyVnd);
            fact.setSellPrice(sellVnd);
            fact.setUnit("VNĐ/Lượng");

            // SourceDimension: PNJ/SJC là loại vàng, còn nguồn bạn có thể đặt "Crawl
            // PNJ/SJC"
            SourceDimension src = sourceDimensionRepository.findBySourceName("Crawl PNJ/SJC")
                    .orElseGet(() -> {
                        SourceDimension s = new SourceDimension();
                        s.setSourceName("Crawl PNJ/SJC");
                        s.setSourceUrl("https://www.giavang.pnj.com.vn/");
                        s.setDescription("Dữ liệu crawl PNJ/SJC theo khu vực/giờ");
                        return sourceDimensionRepository.save(s);
                    });
            fact.setSourceDimension(src);

            // GoldTypeDimension theo "type_name" (PNJ, SJC, …)
            GoldTypeDimension goldType = goldTypeService.getOrCreateGoldType(type);
            fact.setGoldTypeDimension(goldType);

            // LocationDimension: city/region từ branch
            String normBranch = Optional.ofNullable(branch)
                    .orElse("")
                    .trim()
                    .toLowerCase();

            String city = "Unknown";
            String region = "Unknown";
            double bestScore = 0.0;

            // 🎯 Tìm tỉnh có similarity cao nhất
            for (Map.Entry<String, String> e : LocationMap.regionMap.entrySet()) {
                double sim = LevenshteinUtil.levenshteinSimilarity(normBranch, e.getKey().toLowerCase());
                if (sim > bestScore) {
                    bestScore = sim;
                    city = LocationMap.capitalizeWords(e.getKey());
                    region = e.getValue();
                }
            }

            // 🎯 Ngưỡng 0.75: nếu không đủ tương đồng, fallback bằng từ khóa “miền”
            if (bestScore < 0.75) {
                if (normBranch.contains("miền tây")) {
                    region = "Miền Tây";
                    city = "Unknown";
                } else if (normBranch.contains("tây nguyên")) {
                    region = "Tây Nguyên";
                    city = "Unknown";
                } else if (normBranch.contains("miền bắc")) {
                    region = "Miền Bắc";
                    city = "Unknown";
                } else if (normBranch.contains("miền trung")) {
                    region = "Miền Trung";
                    city = "Unknown";
                } else if (normBranch.contains("miền nam") ||
                        normBranch.contains("tphcm") ||
                        normBranch.contains("tp hcm") ||
                        normBranch.contains("ho chi minh") ||
                        normBranch.contains("hcm")) {
                    city = "Hồ Chí Minh";
                    region = "Miền Nam";
                }
            }

            // 🧾 Debug log
            log.debug("🗺️ Chuẩn hoá location='{}' → city='{}', region='{}' (score={})",
                    branch, city, region, String.format("%.2f", bestScore));

            // 🔍 Tìm trong DB hoặc tạo mới
            Optional<LocationDimension> existingLoc;
            if (!"Unknown".equals(city) && !"Unknown".equals(region)) {
                existingLoc = locationDimensionRepository.findByCityIsAndRegionIs(city, region);
            } else if (!"Unknown".equals(city)) {
                existingLoc = locationDimensionRepository.findByCityIs(city);
            } else if (!"Unknown".equals(region)) {
                existingLoc = locationDimensionRepository.findByRegionIs(region).stream().findFirst();
            } else {
                existingLoc = Optional.empty();
            }

            String finalCity = city;
            String finalRegion = region;
            LocationDimension loc = existingLoc.orElseGet(() -> {
                LocationDimension l = new LocationDimension();
                l.setCity(finalCity);
                l.setRegion(finalRegion);
                return locationDimensionRepository.save(l);
            });
            fact.setLocationDimension(loc);

            // TimeDimension: lấy ngày từ "yyyyMMdd", giờ từ updated_at nếu có, fallback 12h
            LocalDate d = LocalDate.parse(dateRaw, DateTimeFormatter.ofPattern("yyyyMMdd"));
            int hour = 12;
            if (updated != null && !updated.isBlank()) {
                try {
                    LocalDateTime ldt = LocalDateTime.parse(
                            updated,
                            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
                    hour = ldt.getHour();
                } catch (Exception ignore) {
                }
            }

            int finalHour = hour;
            TimeDimension time = timeDimensionRepository
                    .findByDayIsAndMonthIsAndYearIsAndHourIs(d.getDayOfMonth(), d.getMonthValue(), d.getYear(), hour)
                    .orElseGet(() -> {
                        TimeDimension t = new TimeDimension();
                        t.setDateTime(d);
                        t.setDay(d.getDayOfMonth());
                        t.setMonth(d.getMonthValue());
                        t.setYear(d.getYear());
                        t.setHour(finalHour);
                        return timeDimensionRepository.save(t);
                    });
            fact.setTimeDimension(time);

            return fact;
        } catch (Exception e) {
            log.warn("⚠️ convertNestedRowToEntity lỗi: {}", e.getMessage());
            return null;
        }
    }

    private long parsePriceVnd(String s) {
        if (s == null)
            return 0L;
        // bỏ dấu . , và khoảng trắng
        String digits = s.replace(".", "").replace(",", "").replace(" ", "");
        if (digits.isEmpty() || !digits.matches("\\d+"))
            return 0L;
        long thousands = Long.parseLong(digits); // ví dụ "66400" nghìn
        return thousands * 1000L; // -> 66,400,000 VNĐ
    }
}
