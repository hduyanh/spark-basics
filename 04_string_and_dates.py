"""
1. Working with String Data
    1. Case When, Regex_Replace etc
2. Working with Dates
    1. to_date, current_date, current_timestamp
3. Working with NULL values
    1. nvl, na.drop, na.fill
"""

from pyspark.sql import SparkSession
import time

spark = (
    SparkSession
    .builder
    .appName("Working with Strings & Dates")
    .master("local[*]")
    .getOrCreate()
)

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
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
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

# Case When
# select employee_id, name, age, salary, gender,
# case when gender = "Male" then "M" when gender = "Female" then "F" else null end as new_gender, hire_date from emp
from pyspark.sql.functions import when, col, expr
emp_gender_fixed = emp.withColumn("new_gender", when(col("gender")== "Male", "M")
                                 .when(col("gender") == "Female", "F")
                                 .otherwise(None)
                                 ) 
emp_gender_fixed.show()
#expr method 
emp_gender_fixed_1 = emp.withColumn("new_gender", expr("case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end"))
emp_gender_fixed_1.show()

# Replace in Strings
# select employee_id, name, replace(name, "J", "Z") as new_name, age, salary, gender, new_gender, hire_date from emp_gender_fixed
from pyspark.sql.functions import regexp_replace
emp_name_fixed = emp_gender_fixed.withColumn("new_name", regexp_replace(col("name"), "J", "Z"))
emp_name_fixed.show()

# Convert Date
# select *,  to_date(hire_date, "YYYY-MM-DD") as hire_date from emp_name_fixed
from pyspark.sql.functions import to_date
emp_date_fix = emp_name_fixed.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))
emp_date_fix.show()
emp_date_fix.printSchema()

# Add Date Columns
# Add current_date, current_timestamp, extract year from hire_date
from pyspark.sql.functions import current_date, current_timestamp
emp_dated = emp_date_fix.withColumn("date_now", current_date()).withColumn("timestamp_now", current_timestamp())
emp_dated.show(truncate=False) 

# Drop Null gender records
emp_1 = emp_dated.na.drop()
emp_1.show()

# Fix Null values
# select *, nvl("new_gender", "O") as new_gender from emp_dated
from pyspark.sql.functions import coalesce, lit
emp_null_df = emp_dated.withColumn("new_gender", coalesce(col("new_gender"), lit("O")))
emp_null_df.show()

# Drop old columns and Fix new column names
emp_final = emp_null_df.drop("name", "gender").withColumnRenamed("new_name", "name").withColumnRenamed("new_gender", "gender")
emp_final.show(truncate=False)
emp_final.printSchema()

# Write data as CSV
#emp_final.write.format("csv").save("result/emp3.csv")


# Bonus TIP
# Convert date into String and extract date information
from pyspark.sql.functions import date_format
emp_fixed = emp_final.withColumn("date_string", date_format(col("hire_date"), "dd/MM/yyyy"))
emp_fixed_1 = emp_final.withColumn("date_year", date_format(col("hire_date"), "yyyy"))
emp_fixed_2 = emp_final.withColumn("date_year", date_format(col("timestamp_now"), "z"))
emp_fixed.show()
emp_fixed_1.show()
emp_fixed_2.show()

"""
Documentation Date and Time
https://spark.apache.org/docs/3.5.3/sql-ref-datetime-pattern.html
"""

time.sleep(300)