"""
1. Adding Columns
2. Using Literals/Static values
3. Renaming Columns
4. Removing Columns
5. Filtering and LIMIT for DataFrame
6. Structured Transformations - withColumn, withColumnRenamed, lit
"""

from pyspark.sql import SparkSession
import time

spark = (
    SparkSession
    .builder
    .appName("Basic Transformation - II")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Web UI:", spark.sparkContext.uiWebUrl)

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

# Show data
emp.show()

# Schema for emp
emp.printSchema()

# Casting Column
# select employee_id, name, age, cast(salary as double) as salary from emp
from pyspark.sql.functions import col, cast
emp_casted = emp.select("employee_id", "name", "age", col("salary").cast("double"))
emp_casted.printSchema()
emp_casted.show()

# Adding Columns
# select employee_id, name, age, salary, (salary * 0.2) as tax from emp_casted
emp_taxed = emp_casted.withColumn("tax", col("salary")* 0.2)
emp_taxed.show()

# Literals
# select employee_id, name, age, salary, tax, 1 as columnOne, "two" as columnTwo from emp_taxed
from pyspark.sql.functions import lit
emp_new_cols = emp_taxed.withColumn("columnOne", lit(1)).withColumn("columnTwo", lit("two"))
emp_new_cols.show()

# Renaming Columns
# select employee_id as emp_id, name, age, salary, tax, columnOne, columnTwo from emp_new_cols
emp_1 = emp_new_cols.withColumnRenamed("employee_id", "emp_id")
emp_1.show()

# Column names with Spaces
# select employee_id as emp_id, name, age, salary, tax, columnOne, columnTwo as `Column Two` from emp_new_cols
emp_2 = emp_new_cols.withColumnRenamed("columnTwo", "column Two")
emp_2.show()

# Remove Column
emp_dropped = emp_new_cols.drop("columnTwo", "columnOne")
emp_dropped.show()

# Filter data 
# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_col_dropped where tax > 1000
emp_filtered = emp_dropped.where("tax > 10000")
emp_filtered.show()

# LIMIT data
# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_filtered limit 5
emp_limit = emp_filtered.limit(5)

# Show data
emp_limit.show(2)

# Bonus TIP
# Add multiple columns
columns = {
    "tax" : col("salary") * 0.2,
    "oneNumber": lit(1),
    "columnTwo": lit("two")
}

emp_final = emp.withColumns(columns)
emp_final.show()

time.sleep(300)