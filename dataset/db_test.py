from pyspark.sql import SparkSession
import sys

# Fetch command-line arguments
db_user = sys.argv[1]
db_password = sys.argv[2]

print("Job started...")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgreSQLConnectionTest") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Database configuration
db_url = "jdbc:postgresql://tokamak-db-ro-replica.c556tc2cxz4l.us-east-1.rds.amazonaws.com:5432/oli"
db_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Test query
query = "(SELECT COUNT(*) FROM public.users) AS user_count"

# Read from PostgreSQL
df = spark.read.jdbc(url=db_url, table=query, properties=db_properties)

# Show the result
df.show()

# Print the count to the console
count = df.collect()[0][0]
print(f"User count: {count}")

# Stop the Spark session
spark.stop()
