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
    schema="de_silver")
cur = snow_conn.cursor()


# USERS_EXTRACT -> DIM_USERS
cur.execute("""
                create table if not exists "MYDBT"."DE_GOLD"."DIM_USERS"(
                    sk number not null autoincrement primary key,
                    id number not null,
                    name varchar not null,
                    age number not null,
                    address varchar not null,
                    created_at datetime not null,
                    valid_from datetime not null default current_timestamp,
                    is_effective boolean not null default TRUE                
                )
            """)
cur.execute(f"""
         merge into "MYDBT"."DE_GOLD"."DIM_USERS" dim_user
            using (
                select  *
                from "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"                                    
            ) extracted
            on extracted.id = dim_user.id
            when matched then
                update set
                  dim_user.is_effective = FALSE            
        """)

cur.execute("""
        insert into "MYDBT"."DE_GOLD"."DIM_USERS"(id, name, age, address, created_at) 
            select id, name, age, address, created_at
            from "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"           
""")

cur.close()