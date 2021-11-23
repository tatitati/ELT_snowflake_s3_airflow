#!/usr/local/bin/python3

from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
from snowflake.connector import ProgrammingError

jarPath='/opt/airflow/elt/jars'
jars = [
    # spark-snowflake
    f'{jarPath}/spark-snowflake/snowflake-jdbc-3.13.10.jar',
    f'{jarPath}/spark-snowflake/spark-snowflake_2.12-2.9.2-spark_3.1.jar', # scala 2.12 + pyspark 3.1
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {",".join(jars)}  pyspark-shell'
context = SparkContext(master="local[*]", appName="readJSON")
app = SparkSession.builder.appName("myapp").getOrCreate()

parser = configparser.ConfigParser()
parser.read("/opt/airflow/elt/pipeline.conf")
snowflake_username = parser.get("snowflake_credentials", "username")
snowflake_password = parser.get("snowflake_credentials", "password")
snowflake_account_name = parser.get("snowflake_credentials", "account_name")
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_bronze")
cur = snow_conn.cursor()

tables = ['ORDERS', 'USERS']

for table in tables:
    cur.execute(f"""create table if not exists "MYDBT"."DE_SILVER"."audit__{table}_CURRENT"(
                            id number not null primary key autoincrement,
                            records_in_file number not null,
                            source_file varchar not null,                            
                            copied_at datetime not null
                );""")

    current_sql = f"""
        insert into "MYDBT"."DE_SILVER"."audit__{table}_CURRENT"(records_in_file, source_file, copied_at)
            select count(*), source, created_at as copied_at
            from "MYDBT"."DE_BRONZE"."{table}_CURRENT"
            group by source, created_at;
    """
    cur.execute(current_sql)

cur.close()