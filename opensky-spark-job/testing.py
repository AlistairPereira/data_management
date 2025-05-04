from pyspark.sql import SparkSession

# Initialize Spark session with BigQuery support
spark = SparkSession.builder \
    .appName("GCS to BigQuery") \
    .config("spark.jars.packages", 
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

# Replace with your values
gcs_path = "gs://opensky-raw-data-bucket/incoming-data/*.json"
bigquery_table = "data-management-2-457212.opensky_data.flight_states_staging"

# Read JSON files from GCS
df = spark.read.option("multiline", "true").json(gcs_path)

# (Optional) Preview the data
df.printSchema()
df.show(5)

# Write data to BigQuery
df.write.format("bigquery") \
    .option("table", bigquery_table) \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()

spark.stop()


print("------------------")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GCS to BigQuery Handling Multiple JSON Types") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

# GCS input path and BigQuery table
gcs_path = "gs://opensky-raw-data-bucket/incoming-data/*.json"
bq_table = "data-management-2-457212.opensky_data.flight_states_staging"

# Read raw JSON from GCS
raw_df = spark.read.option("multiline", "true").json(gcs_path)

# Identify rows based on 'data_type'
flight_data_df = raw_df.filter(col("data_type") == "flight_data")
flight_state_df = raw_df.filter(col("data_type") == "flight_state")

# Process "flight_state" rows:
# - Each "states" is an array of arrays => we need to explode it.
if flight_state_df.count() > 0:
    exploded_states_df = flight_state_df.select(
        explode(col("states")).alias("state_info"),
        col("time").alias("state_time"),
        col("data_type")
    )

    # Now map the exploded array into individual columns
    exploded_states_df = exploded_states_df.select(
        col("state_info")[0].alias("icao24"),
        col("state_info")[1].alias("callsign"),
        col("state_info")[2].alias("origin_country"),
        col("state_info")[3].alias("time_position"),
        col("state_info")[4].alias("last_contact"),
        col("state_info")[5].alias("longitude"),
        col("state_info")[6].alias("latitude"),
        col("state_info")[7].alias("baro_altitude"),
        col("state_info")[8].alias("on_ground"),
        col("state_info")[9].alias("velocity"),
        col("state_info")[10].alias("heading"),
        col("state_info")[11].alias("vertical_rate"),
        col("state_info")[12].alias("sensors"),
        col("state_info")[13].alias("geo_altitude"),
        col("state_info")[14].alias("squawk"),
        col("state_info")[15].alias("spi"),
        col("state_info")[16].alias("position_source"),
        col("state_time").alias("time"),
        col("data_type")
    )

# Add missing columns to both DataFrames so schemas match
# For flight_data_df, add missing columns with null values
flight_data_df = flight_data_df.withColumn("origin_country", lit(None)) \
    .withColumn("time_position", lit(None)) \
    .withColumn("last_contact", lit(None)) \
    .withColumn("longitude", lit(None)) \
    .withColumn("latitude", lit(None)) \
    .withColumn("baro_altitude", lit(None)) \
    .withColumn("on_ground", lit(None)) \
    .withColumn("velocity", lit(None)) \
    .withColumn("heading", lit(None)) \
    .withColumn("vertical_rate", lit(None)) \
    .withColumn("sensors", lit(None)) \
    .withColumn("geo_altitude", lit(None)) \
    .withColumn("squawk", lit(None)) \
    .withColumn("spi", lit(None)) \
    .withColumn("position_source", lit(None)) \
    .withColumn("time", col("firstSeen")) \
    .select(
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "heading", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source", "time", "data_type"
    )

# If flight_state_df exists, union both datasets
if flight_state_df.count() > 0:
    final_df = flight_data_df.unionByName(exploded_states_df)
else:
    final_df = flight_data_df

# (Optional) Preview
final_df.printSchema()
final_df.show(5)

# Write the final merged dataframe to BigQuery
final_df.write.format("bigquery") \
    .option("table", bq_table) \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()

spark.stop()

print("-------main----")


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import logging
import sys

# Setup basic logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GCS to BigQuery Handling Multiple JSON Types") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

# Config
gcs_path = "gs://opensky-raw-data-bucket/incoming-data/*.json"
bq_dataset = "opensky_data"
bq_table_flight_state = "flight_states_staging"
bq_table_flight_data = "flight_data_staging"
project_id = "data-management-2-457212"

logger.info("Reading JSON files from GCS...")
try:
    df = spark.read.option("multiline", "true").json(gcs_path)
    df.printSchema()

    logger.info("Schema loaded:")
    logger.info(df.schema)

    # Separate the two types
    logger.info("Processing flight_state data...")
    flight_state_df = df.filter(col("data_type") == "flight_state")

    # Flatten 'states' array
    flight_state_flattened = flight_state_df.selectExpr(
        "states[0][0] as icao24",
        "states[0][1] as callsign",
        "states[0][2] as origin_country",
        "states[0][3] as time_position",
        "states[0][4] as last_contact",
        "states[0][5] as longitude",
        "states[0][6] as latitude",
        "states[0][7] as baro_altitude",
        "states[0][8] as on_ground",
        "states[0][9] as velocity",
        "states[0][10] as heading",
        "states[0][11] as vertical_rate",
        "states[0][12] as sensors",
        "states[0][13] as geo_altitude",
        "states[0][14] as squawk",
        "states[0][15] as spi",
        "states[0][16] as position_source",
        "time",
        "data_type"
    )

    flight_state_flattened.write.format("bigquery") \
        .option("table", f"{project_id}.{bq_dataset}.{bq_table_flight_state}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

    logger.info("✅ Successfully inserted flight_state data!")

    # Now for flight_data
    logger.info("Processing flight_data...")

    flight_data_df = df.filter(col("data_type") == "flight_data")

    # Only select columns that exist
    available_cols = flight_data_df.columns

    selected_cols = []
    for field in ["icao24", "callsign", "estDepartureAirport", "estArrivalAirport", "firstSeen", "lastSeen", "time", "data_type"]:
        if field in available_cols:
            selected_cols.append(field)
        else:
            logger.warning(f"Skipping missing column: {field}")

    flight_data_selected = flight_data_df.select(*selected_cols)

    flight_data_selected.write.format("bigquery") \
        .option("table", f"{project_id}.{bq_dataset}.{bq_table_flight_data}") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

    logger.info("✅ Successfully inserted flight_data!")

except Exception as e:
    logger.error(f"❌ Error occurred: {str(e)}")

finally:
    spark.stop()

