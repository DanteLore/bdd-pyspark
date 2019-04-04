import random
import re
import string
import inflect

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


def random_cell(field_type, mode):
    r = re.compile(r"^.*RAND(\(([0-9]+)-([0-9]+)\))?.*$")

    (_, l, u) = r.match(mode).groups()

    lower = int(l) if l else 0
    upper = int(u) if u else 2147483647

    t = field_type.lower()
    if t == "int" or t == "long":
        return random.randint(lower, upper)
    else:
        return ''.join(random.choices(string.ascii_lowercase, k=24))


def seq_cell(sequence_positions, inflect_engine, name, ftype):
    val = sequence_positions.get(name, 0) + 1
    sequence_positions[name] = val

    dt = ftype.lower()
    if dt in ["int", "long"]:
        return val
    elif dt in ["string", "str"]:
        return inflect_engine.number_to_words(val)
    else:
        raise ValueError("Data type not supported for sequences {0}".format(dt))


def auto_row(sequence_positions, inflect_engine, cols):
    for (name, ftype, mode) in cols:
        if "RAND" in mode:
            yield random_cell(ftype, mode)
        elif mode == "SEQ":
            yield seq_cell(sequence_positions, inflect_engine, name, ftype)


def process_wildcards(cols, cells):
    data = list(zip(cols, cells))
    for ((_, ftype), cell) in data:
        if "%RAND" in cell:
            yield random_cell(ftype, cell)
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
    sequence_positions = {}
    inflect_engine = inflect.engine()
    cols = [(row['name'], row['type'], row['mode']) for row in config]
    schema = T.StructType([T.StructField(name, string_to_type(ftype), False) for (name, ftype, _) in cols])
    rows = [list(auto_row(sequence_positions, inflect_engine, cols)) for _ in range(0, int(row_count))]
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