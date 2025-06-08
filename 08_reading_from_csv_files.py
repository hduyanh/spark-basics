"""
1. Reading Data from CSV Files
2. Understand Background Working using Spark UI
3. Handing BAD records
4. Spark Read Modes (PERMISSIVE, DROPMALFORMED, FAILFAST)
Document: https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading from CSV Files")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Web UI:", spark.sparkContext.uiWebUrl)

# Read a csv file into dataframe
df_0 = spark.read.format("csv").load("examples/emp.csv") # no header
df_1 = spark.read.format("csv").option("header", True).load("examples/emp.csv")
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("examples/emp.csv") # inferSchema: let spark identify the data type of column, based on records
df.printSchema() # spark job was triggered, to identify the metadata
df.show()

# Reading with Schema
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date" # no job for meta data if we already give schema
df_schema = spark.read.format("csv").option("header", True).schema(_schema).load("examples/emp.csv")
df_schema.show()

# Handle BAD records - PERMISSIVE (Default mode)
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date, bad_record string"
df_p = spark.read.format("csv").schema(_schema).option("columnNameOfCorruptRecord", "bad_record").option("header", True).load("datasets/emp_new.csv")
df_p.printSchema()
df_p.show()
#df_p.where("_corrupt_record is null").show()

# Handle BAD records - DROPMALFORMED
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date" # no job for meta data if we already give schema
df_m = spark.read.format("csv").option("header", True).option("mode", "DROPMALFORMED").schema(_schema).load("datasets/emp_new.csv")
df_m.printSchema()
df_m.show()
# Handle BAD records - FAILFAST

# BONUS TIP
# Multiple options

input("Press Enter to exit...\n")