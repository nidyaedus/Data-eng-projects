from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

def main():
    print("1. Spark Başlatılıyor...")
    # Airflow bu dosyayı tetiklediğinde Spark motoru sıfırdan ayağa kalkacak
    spark = SparkSession.builder \
        .appName("NYC Taxi Production ETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://datalake-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("2. Veri 'Landing' Katmanından Okunuyor...")
    # Tüm parquet dosyalarını okuyoruz (Extract)
    df_raw = spark.read.parquet("s3a://landing/*.parquet")

    print("3. İş Kuralları Uygulanıyor ve Temizleniyor...")
    # Hatalı verileri (eksi ücret, sıfır yolcu) filtreliyoruz (Transform)
    df_clean = df_raw.filter(
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0.0) &
        (col("fare_amount") > 0.0)
    ).dropna(subset=["passenger_count", "trip_distance", "fare_amount", "tpep_pickup_datetime"])

    print("4. Bölümleme Yapılıyor ve 'Curated' Katmanına Yazılıyor...")
    # Yıl ve ay kolonlarını oluşturup Partitioning ile kaydediyoruz (Load)
    df_final = df_clean.withColumn("year", year("tpep_pickup_datetime")) \
                       .withColumn("month", month("tpep_pickup_datetime"))

    df_final.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("s3a://curated/nyc_taxi_data/")

    print("ETL Süreci Tamamlandı")
    spark.stop() # Hafızayı serbest bırakmak için sistemi kapatıyoruz


if __name__ == "__main__":
    main()