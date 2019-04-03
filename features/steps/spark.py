from behave import *
from pyspark.sql import SparkSession


@given('a spark session')
def step_impl(context):
    context.spark = (
        SparkSession.builder
            .master("local")
            .appName("TFL Notebook")
            .config('spark.executor.memory', '8G')
            .config('spark.driver.memory', '16G')
            .config('spark.driver.maxResultSize', '10G')
            .getOrCreate()
    )

