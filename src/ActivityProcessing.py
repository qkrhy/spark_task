import os
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, to_timestamp
from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp

def initialize_spark():
    spark = SparkSession.builder \
        .appName("UserActivityProcessing") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.port", "7077") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    return spark

# 처리된 기간을 로드하는 함수 (중복 제외, 특정 기간만 처리 가능)
def load_processed_periods(checkpoint_dir):
    processed_periods_file = os.path.join(checkpoint_dir, "processed_periods.txt")
    if os.path.exists(processed_periods_file):
        with open(processed_periods_file, 'r') as file:
            return {line.strip() for line in file}
    return set()

# 처리된 기간을 체크포인트 디렉토리에 저장하는 함수
def save_processed_period(checkpoint_dir, year, month):
    processed_periods_file = os.path.join(checkpoint_dir, "processed_periods.txt")
    with open(processed_periods_file, 'a') as file:
        file.write(f"{year}-{month}\n")

# External Table 생성 함수
def create_external_table(spark, table_name, table_location):
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
            event_time STRING,
            event_type STRING,
            product_id STRING,
            category_id STRING,
            category_code STRING,
            brand STRING,
            price DOUBLE,
            user_id STRING,
            user_session STRING
        )
        PARTITIONED BY (event_date STRING)
        STORED AS PARQUET
        LOCATION '{table_location}'
    """)

# 데이터 처리 함수
def process_data(spark, start_year, start_month, end_year, end_month, checkpoint_dir, table_name, table_location):
    # 이미 처리된 기간을 로드
    processed_periods = load_processed_periods(checkpoint_dir)

    # External Table 생성
    create_external_table(spark, table_name, table_location)

    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        period = f"{year}-{calendar.month_abbr[month]}"
        if period in processed_periods:
            print(f"{period} already processed. Skipping.")
        else:
            try:
                data_path = f'../data/{year}-{calendar.month_abbr[month]}.csv'
                if not os.path.exists(data_path):
                    print(f"Data file for {period} not found. Skipping.")
                else:
                    month_sdf = spark.read.csv(data_path, header=True, inferSchema=True)
                    
                    # timezone functions 적용 전 'event_time'을 타임스탬프로 변환
                    month_sdf = month_sdf.withColumn("event_time", to_timestamp(month_sdf["event_time"], "yyyy-MM-dd HH:mm:ss"))

                    # UTC 변환하고, KST로 변환
                    month_sdf = month_sdf.withColumn("event_time_utc", to_utc_timestamp(month_sdf["event_time"], "UTC"))
                    month_sdf = month_sdf.withColumn("event_time_kst", from_utc_timestamp(month_sdf["event_time_utc"], "Asia/Seoul"))

                    # event_date
                    month_sdf = month_sdf.withColumn("event_date", to_date(month_sdf["event_time_kst"], "yyyy-MM-dd"))
                    
                    # 컬럼 선택                    
                    month_sdf = month_sdf.select("event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session", "event_date")
                    
                    # External Table에 데이터 삽입 (partitionBy 제거)
                    month_sdf.write \
                        .mode("append") \
                        .format("parquet") \
                        .option("compression", "snappy") \
                        .insertInto(table_name)
                    # 처리된 기간을 저장
                    save_processed_period(checkpoint_dir, year, calendar.month_abbr[month])
                    processed_periods.add(period)
            except Exception as e:
                print(f"Failed to process data for {period}. Error: {str(e)}")

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

def main():
    checkpoint_dir = "../check"
    table_name = "user_activity_log"
    table_location = "/Users/qkrgyqls/Desktop/03_spark/activity"

    spark = initialize_spark()
    # 2019년 1월부터 2019년 12월까지 데이터 처리
    process_data(spark, 2019, 1, 2019, 12, checkpoint_dir, table_name, table_location)

if __name__ == "__main__":
    main()