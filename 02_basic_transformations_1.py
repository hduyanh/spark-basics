"""
1. DataFrame Columns
2. DataFrame Rows
3. DataFrame Schema
4. Structured Transformations - select, expr, selecExpr, cast
"""

from pyspark.sql import SparkSession
#import code
import time

spark = (
    SparkSession
    .builder
    .appName("Basic Transformation - I")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Web UI:", spark.sparkContext.uiWebUrl)
#spark.sparkContext.setLogLevel("DEBUG")
spark.sparkContext.setLogLevel("ERROR")

# Emp Data & Schema
emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# Create emp DataFrame
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)

# Check number of partitions
nop = emp.rdd.getNumPartitions()

# Show data
emp.show()

# Schema for emp
emp.printSchema()
# or
emp_sch = emp.schema
#print(emp_sch)

# Example for Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema_string = "name string, age int"
schema_spark = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
#print(schema_spark)

# Columns and expression
from pyspark.sql.functions import col, expr
emp["salary"]

# SELECT columns
# select employee_id, name, age, salary from emp
emp_filtered = emp.select(col("employee_id"), expr("name"), emp.age, emp.salary)
emp_filtered.show()
emp_filtered.printSchema()

# Using expr for select
# select employee_id as emp_id, name, cast(age as int) as age, salary from emp_filtered
emp_casted = emp_filtered.select(expr("employee_id as emp_id"), emp.name, expr("cast(age as int) as age"), emp.salary)
emp_casted.show()
emp_casted.printSchema()

# Select expr method
emp_casted_1 = emp_filtered.selectExpr("employee_id as emp_id", "name", "cast(age as int) as age", "salary")
emp_casted_1.show()

# Filter emp based on Age > 30
# select emp_id, name, age, salary from emp_casted where age > 30
emp_final = emp_casted.select("emp_id", "name", "age", "salary").where("age > 30")
emp_final.show()

# Write the data back as CSV (ACTION)
#emp_final.write.format("csv").save("result/emp2.csv")

# Bonus
schema_str = "name string, age int"
from pyspark.sql.types import _parse_datatype_string
schema_spark = _parse_datatype_string(schema_str)
print(schema_spark)

time.sleep(300) # let run for X amount of time
#input("Press Enter to exit...\n") # let run till press enter
#code.interact(local=locals()) # let run till exit