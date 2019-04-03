import random
import string

from behave import *
from pyspark.sql import types as T


def string_to_type(s):
    tname = s.lower()
    if tname == "int":
        return T.IntegerType()
    elif tname == "long":
        return T.LongType()
    else:
        return T.StringType()


def random_cell(s):
    tname = s.lower()
    if tname == "int":
        return random.getrandbits(31)
    elif tname == "long":
        return random.getrandbits(63)
    else:
        return ''.join(random.choices(string.ascii_lowercase, k=24))


def seq_cell(seq_map, name, ftype):
    if ftype.lower() not in ["int", "long"]:
        raise ValueError("Sequences only work on integer types")

    val = seq_map.get(name, 0) + 1
    seq_map[name] = val

    return val


def auto_row(seq_map, cols):
    for (name, ftype, mode) in cols:
        if mode == "RAND":
            yield random_cell(ftype)
        elif mode == "SEQ":
            yield seq_cell(seq_map, name, ftype)


def process_wildcards(cols, cells):
    data = list(zip(cols, cells))
    for ((_, ftype), cell) in data:
        if "%RAND%" in cell:
            yield random_cell(ftype)
        else:
            yield cell


def table_to_spark(spark, table):
    cols = [h.split(':') for h in table.headings]
    schema = T.StructType([T.StructField(name + "_str", T.StringType(), False) for (name, _) in cols])
    rows = [list(process_wildcards(cols, row.cells)) for row in table]
    df = spark.createDataFrame(rows, schema=schema)

    for (name, field_type) in cols:
        df = (
            df
                .withColumn(name, df[name + "_str"].cast(string_to_type(field_type)))
                .drop(name + "_str")
        )

    return df


def generate_random_table(spark, config, row_count):
    seq_map = {}
    cols = [(row['name'], row['type'], row['mode']) for row in config]
    schema = T.StructType([T.StructField(name, string_to_type(ftype), False) for (name, ftype, _) in cols])
    rows = [list(auto_row(seq_map, cols)) for _ in range(0, int(row_count))]
    df = spark.createDataFrame(rows, schema=schema)
    return df


@given(u'a table called "{table_name}" containing')
def step_impl(context, table_name):
    df = table_to_spark(context.spark, context.table)
    df.createOrReplaceTempView(table_name)


@given(u'a table called "{table_name}" containing "{row_count}" rows with schema')
def step_impl(context, table_name, row_count):
    df = generate_random_table(context.spark, context.table, row_count)
    df.createOrReplaceTempView(table_name)


@then(u'the table "{table_name}" contains')
def step_impl(context, table_name):
    expected_df = table_to_spark(context.spark, context.table)
    actual_df = context.spark.sql("select * from {0}".format(table_name))

    # print("\n\n\nEXPECTED:")
    # expected_df.show()
    # print("ACTUAL:")
    # actual_df.show()

    assert (expected_df.schema == actual_df.schema)
    assert (expected_df.subtract(actual_df).count() == 0)
    assert (actual_df.subtract(expected_df).count() == 0)