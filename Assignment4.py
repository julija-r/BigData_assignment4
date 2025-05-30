from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, lag, radians, sin, cos, atan2, sqrt, unix_timestamp, sum as spark_sum
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

spark = SparkSession.builder.appName("VesselRoute").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow").getOrCreate()

# Load the CSV file into a DataFrame
file_path = r"\Data\aisdk-2024-05-04.csv"

schema = StructType([
    StructField("# Timestamp", StringType(), True),
    StructField("Type of mobile", StringType(), True),
    StructField("MMSI", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Navigational status", StringType(), True),
    StructField("ROT", StringType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True),
    StructField("Heading", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("Callsign", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Ship type", StringType(), True),
    StructField("Cargo type", StringType(), True),
    StructField("Width", StringType(), True),
    StructField("Length", StringType(), True),
    StructField("Type of position fixing device", StringType(), True),
    StructField("Draught", StringType(), True),
    StructField("Destination", StringType(), True),
    StructField("ETA", StringType(), True),
    StructField("Data source type", StringType(), True),
    StructField("Size A", StringType(), True),
    StructField("Size B", StringType(), True),
    StructField("Size C", StringType(), True),
    StructField("Size D", StringType(), True)
])

# Read the file as DataFrame
df = spark.read.schema(schema).option("header", True).csv(file_path)
print("df head", df.head())

### Data cleaning

# Filter ou  vehicles that are not moving based on status
df = df.filter(~col("Navigational status").isin(["Moored", "At anchor", "Reserved for future use"]))

# Drop rows with nulls in required fields
df = df.dropna(subset=["MMSI", "Latitude", "Longitude", "# Timestamp", "SOG", "COG"])

# Convert SOG and COG to numeric (Double)
# df = df.withColumn("SOG", col("SOG").cast(DoubleType()))
# df = df.withColumn("COG", col("COG").cast(DoubleType()))

# Drop rows where casting failed
df = df.dropna(subset=["SOG", "COG"])

# Filter out invalid latitude/longitude values
df = df.filter((col("Latitude") >= -90) & (col("Latitude") <= 90) &
               (col("Longitude") >= -180) & (col("Longitude") <= 180))

# Filter out rows with non-positive speed over ground
df = df.filter(col("SOG") > 0)

df.printSchema()

# Convert types for latitude, longitude, and timestamp
df = df.withColumn("Latitude", col("Latitude")) \
           .withColumn("Longitude", col("Longitude")) \
           .withColumn("Timestamp", to_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))

# Define a window specification by MMSI ordered by Timestamp
windowSpec = Window.partitionBy("MMSI").orderBy("Timestamp")

# Get previous coordinates and timestamp
df = df.withColumn("prev_lat", lag("Latitude").over(windowSpec)) \
       .withColumn("prev_lon", lag("Longitude").over(windowSpec))

# Get previous timestamp as well
from pyspark.sql.functions import unix_timestamp

df = df.withColumn("prev_time", lag("Timestamp").over(windowSpec))

# Filter rows where previous values are not null and time difference is valid
df = df.filter(col("prev_lat").isNotNull() & col("prev_lon").isNotNull() & col("prev_time").isNotNull())

# Calculate distance and time delta
# Then compute speed and filter out unrealistic speeds (>24 knots ~ 44.448 km/h)
R = 6371.0  # Earth radius in km
df = df.withColumn("dlat", radians(col("Latitude") - col("prev_lat"))) \
       .withColumn("dlon", radians(col("Longitude") - col("prev_lon"))) \
       .withColumn("a", sin(col("dlat") / 2) ** 2 + cos(radians(col("prev_lat"))) * cos(radians(col("Latitude"))) * sin(col("dlon") / 2) ** 2) \
       .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))) \
       .withColumn("distance_km", col("c") * R) \
       .withColumn("time_diff_hr", (unix_timestamp(col("Timestamp")) - unix_timestamp(col("prev_time"))) / 3600.0) \
       .withColumn("calc_speed_kmh", col("distance_km") / col("time_diff_hr")) \
       .filter(col("calc_speed_kmh") <= 44.448)

# Aggregate total distance per MMSI
total_distance_df = df.groupBy("MMSI").agg(spark_sum("distance_km").alias("total_distance_km"))

# Find the vessel with the longest travelled distance
max_distance_df = total_distance_df.orderBy(col("total_distance_km").desc()).limit(1)

# S the result
max_distance_df.show()

# Stop Spark session
spark.stop()


