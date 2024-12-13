from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour, dayofweek
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Flight Data Analysis") \
    .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
    .getOrCreate()

# Parameter koneksi PostgreSQL
db_url = "jdbc:postgresql://localhost:5432/msib_7"
db_properties = {
    "user": "postgres",
    "password": "post123",
    "driver": "org.postgresql.Driver"
}

# Membaca data dari PostgreSQL
df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "flight_customer_booking") \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

# Menampilkan data awal untuk validasi
print("Data awal:")
df.show(5)

# Masking Nama: Mengubah nama depan dan belakang menjadi format "M***** M****"
def mask_name(full_name):
    if full_name:
        names = full_name.split()
        masked_first_name = names[0][0] + "*" * (len(names[0]) - 1)
        masked_last_name = names[1][0] + "*" * (len(names[1]) - 1)
        return f"{masked_first_name} {masked_last_name}"
    return full_name

# Inisialisasi kunci enkripsi (simpan kunci ini untuk dekripsi)
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Fungsi Enkripsi Email: Membuat format seperti "****@example.com"
def encrypt_email(email):
    if email:
        masked_email = email.split("@")
        return "****@" + masked_email[1]
    return email

# Daftarkan UDF
mask_name_udf = spark.udf.register("mask_name", mask_name, StringType())
encrypt_email_udf = spark.udf.register("encrypt_email", encrypt_email, StringType())

# Terapkan Masking dan Enkripsi
df_transformed = df.withColumn("masked_name", mask_name_udf(col("name"))) \
                   .withColumn("encrypted_email", encrypt_email_udf(col("email"))) \
                   .drop("name", "email")  # Hapus kolom asli jika tidak diperlukan

# Menyusun ulang kolom untuk memastikan masked_name dan encrypted_email di awal
columns_order = ["masked_name", "encrypted_email"] + [col for col in df_transformed.columns if col not in ["masked_name", "encrypted_email"]]
df_transformed = df_transformed.select(*columns_order)

# Tampilkan hasil Masking dan Enkripsi
print("Data setelah masking dan enkripsi:")
df_transformed.show(5, truncate=False)

# Analisis 1: Rute Terpopuler
popular_route = df.groupBy("route") \
    .agg(count("*").alias("total_bookings")) \
    .orderBy(col("total_bookings").desc()) \
    .limit(1)

print("Rute Terpopuler:")
popular_route.show()

# Analisis 2: Waktu Pemesanan Terbaik (Berdasarkan Jam)
best_time = df.groupBy("flight_hour") \
    .agg(count("*").alias("total_bookings")) \
    .orderBy(col("total_bookings").desc()) \
    .limit(5)

print("Waktu Pemesanan Terbaik (Jam):")
best_time.show()

# Analisis 3: Hari Pemesanan Terbaik
best_day = df.groupBy("flight_day") \
    .agg(count("*").alias("total_bookings")) \
    .orderBy(col("total_bookings").desc()) \

print("Hari Pemesanan Terbaik:")
best_day.show()

# MENYIMPAN HASIL KE POSTGRES
# Menyimpan data customer yang sudah dilakukan masking dan enkripsi ke PostgreSQL Database OLAP
df_transformed.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/flight_final_project") \
    .option("dbtable", "masked_customer_booking") \
    .option("user", "postgres") \
    .option("password", "post123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Menyimpan rute terpopuler ke PostgreSQL Database OLAP
popular_route.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/flight_final_project") \
    .option("dbtable", "most_popular_route") \
    .option("user", "postgres") \
    .option("password", "post123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Menyimpan waktu pemesanan terbaik ke PostgreSQL Database OLAP
best_time.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/flight_final_project") \
    .option("dbtable", "best_time") \
    .option("user", "postgres") \
    .option("password", "post123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Menyimpan hari pemesanan terbaik ke PostgreSQL Database OLAP
best_day.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/flight_final_project") \
    .option("dbtable", "best_day") \
    .option("user", "postgres") \
    .option("password", "post123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Stop SparkSession
spark.stop()