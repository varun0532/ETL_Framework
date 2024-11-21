from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import StructType

# df = spark.read.csv(r"E:\ETL Automation\ETL_Framework\files\Contact_info.csv", header=True)

with open(r"E:\ETL Automation\ETL_Framework\schema_files\Contact_info_schema.json", 'r') as schema_file:
    schema = StructType.fromJson(json.load(schema_file))

print(schema)
df = spark.read.schema(schema).csv(r"E:\ETL Automation\ETL_Framework\files\Contact_info.csv", header=True)

df.printSchema()
print(df.schema.json())

df.show(truncate=False)