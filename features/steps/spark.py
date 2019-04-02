from behave import *
from pyspark.sql import SparkSession
from pyspark.sql import types as T


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


@when('I count the words in "{text}"')
def step_impl(context, text):
    words = context.spark.sparkContext.parallelize(text.split(" "))

    context.result = words.count()


@then("the number of words is '{x}'")
def step_impl(context, x):
    assert (context.result == int(x))


def string_to_type(s):
    tname = s.lower()

    if tname == "int":
        return T.IntegerType()
    elif tname == "long":
        return T.LongType()
    else:
        return T.StringType()


def table_to_spark(spark, table):
    cols = [h.split(':') for h in table.headings]
    schema = T.StructType([T.StructField(name + "_str", T.StringType(), False) for (name, _) in cols])
    rows = [row.cells for row in table]
    df = spark.createDataFrame(rows, schema=schema)

    for (name, field_type) in cols:
        df = (
            df
                .withColumn(name, df[name + "_str"].cast(string_to_type(field_type)))
                .drop(name + "_str")
        )

    df.describe()
    return df


@given(u'a table called "{table_name}" containing')
def step_impl(context, table_name):
    df = table_to_spark(context.spark, context.table)
    df.createOrReplaceTempView(table_name)


@when(u'I select rows from "{source_table}" where "{field}" greater than "{threshold}" into table "{dest_table}"')
def step_impl(context, source_table, field, threshold, dest_table):
    df = context.spark.sql("select * from {0} where {1} >= {2}".format(source_table, field, threshold))
    df.createOrReplaceTempView(dest_table)


@when(u'I do the join business logic')
def step_impl(context):
    df = context.spark.sql("""
        select s.id, s.name as student_name, c.name as subject_name
        from students s
        join subjects c on (s.subject_id == c.id)
    """)
    df.createOrReplaceTempView("results")


@then(u'the table "{table_name}" contains')
def step_impl(context, table_name):
    expected_df = table_to_spark(context.spark, context.table)
    actual_df = context.spark.sql("select * from {0}".format(table_name))

    print("\n\n\nEXPECTED:")
    expected_df.show()
    print("ACTUAL:")
    actual_df.show()

    assert (expected_df.schema == actual_df.schema)
    assert (expected_df.subtract(actual_df).count() == 0)
    assert (actual_df.subtract(expected_df).count() == 0)
