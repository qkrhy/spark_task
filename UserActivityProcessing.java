import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class UserActivityProcessing {

    // Sparksession
    public static SparkSession initializeSpark() {
        SparkSession spark = SparkSession.builder()
                .appName("UserActivityProcessing")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");
        return spark;
    }

    // 처리된 기간을 로드하는 함수 (중복 제외, 특정 기간만 처리 가능)
    public static Set<String> loadProcessedPeriods(String checkpointDir) throws IOException {
        Set<String> processedPeriods = new HashSet<>();
        String processedPeriodsFile = checkpointDir + "/processed_periods.txt";
        if (Files.exists(Paths.get(processedPeriodsFile))) {
            try (BufferedReader reader = new BufferedReader(new FileReader(processedPeriodsFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processedPeriods.add(line.trim());
                }
            }
        }
        return processedPeriods;
    }

     // 처리된 기간을 체크포인트 디렉토리에 저장하는 함수
    public static void saveProcessedPeriod(String checkpointDir, int year, int month) throws IOException {
        String processedPeriodsFile = checkpointDir + "/processed_periods.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(processedPeriodsFile, true))) {
            writer.write(year + "-" + month + "\n");
        }
    }

    // External Table 생성 함수
    public static void createExternalTable(SparkSession spark, String tableName, String tableLocation) {
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " (" +
                "event_time STRING, " +
                "event_type STRING, " +
                "product_id STRING, " +
                "category_id STRING, " +
                "category_code STRING, " +
                "brand STRING, " +
                "price DOUBLE, " +
                "user_id STRING, " +
                "user_session STRING) " +
                "PARTITIONED BY (event_date STRING) " +
                "STORED AS PARQUET " +
                "LOCATION '" + tableLocation + "'");
    }

    // 데이터 처리 함수
    public static void processData(SparkSession spark, int startYear, int startMonth, int endYear, int endMonth, String checkpointDir, String tableName, String tableLocation) throws IOException {
        Set<String> processedPeriods = loadProcessedPeriods(checkpointDir);
        createExternalTable(spark, tableName, tableLocation);

        int year = startYear;
        int month = startMonth;
        while (year < endYear || (year == endYear && month <= endMonth)) {
            String period = year + "-" + month;
            if (processedPeriods.contains(period)) {
                System.out.println(period + " already processed. Skipping.");
            } else {
                try {
                    String dataPath = "../data/" + year + "-" + month + ".csv";
                    if (!Files.exists(Paths.get(dataPath))) {
                        System.out.println("Data file for " + period + " not found. Skipping.");
                    } else {
                        Dataset<Row> monthSDF = spark.read().option("header", "true").csv(dataPath);

                        monthSDF = monthSDF.withColumn("event_time", functions.to_timestamp(monthSDF.col("event_time"), "yyyy-MM-dd HH:mm:ss"));
                        monthSDF = monthSDF.withColumn("event_time_utc", functions.to_utc_timestamp(monthSDF.col("event_time"), "UTC"));
                        monthSDF = monthSDF.withColumn("event_time_kst", functions.from_utc_timestamp(monthSDF.col("event_time_utc"), "Asia/Seoul"));
                        monthSDF = monthSDF.withColumn("event_date", functions.to_date(monthSDF.col("event_time_kst"), "yyyy-MM-dd"));
                        
                        monthSDF = monthSDF.select("event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "event_date");

                        monthSDF.write()
                                .mode("append")
                                .format("parquet")
                                .option("compression", "snappy")
                                .insertInto(tableName);

                        saveProcessedPeriod(checkpointDir, year, month);
                        processedPeriods.add(period);
                    }
                } catch (Exception e) {
                    System.out.println("Failed to process data for " + period + ". Error: " + e.getMessage());
                }
            }
            if (month == 12) {
                year++;
                month = 1;
            } else {
                month++;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String checkpointDir = "../check";
        String tableName = "user_activity_log";
        String tableLocation = "/Users/qkrgyqls/Desktop/03_spark/activity";

        SparkSession spark = initializeSpark();
        processData(spark, 2019, 1, 2019, 12, checkpointDir, tableName, tableLocation);
    }
}
