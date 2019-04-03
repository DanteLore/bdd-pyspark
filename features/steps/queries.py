from behave import *


@when(u'I select rows from "{source_table}" where "{field}" greater than "{threshold}" into table "{dest_table}"')
def step_impl(context, source_table, field, threshold, dest_table):
    df = context.spark.sql("select * from {0} where {1} >= {2}".format(source_table, field, threshold))
    df.createOrReplaceTempView(dest_table)


@then(u'the value "{value}" is not present in the field "{field}" of table "{table}"')
def step_impl(context, table, field, value):
    df = context.spark.sql("select * from {0} where {1} = '{2}'".format(table, field, value))
    assert (df.count() == 0)


@then(u'the sum of field "{field_name}" in table "{table_name}" is greater than zero')
def step_impl(context, field_name, table_name):
    df = context.spark.sql("select {0} from {1}".format(field_name, table_name))
    x = df.groupBy().sum().collect()[0][0]
    assert (x > 0)


@then(u'the sum of field "{field_name}" in table "{table_name}" is "{value}"')
def step_impl(context, field_name, table_name, value):
    df = context.spark.sql("select {0} from {1}".format(field_name, table_name))
    x = df.groupBy().sum().collect()[0][0]
    assert (x == int(value))


@then(u'the min of field "{field_name}" in table "{table_name}" is "{value}"')
def step_impl(context, field_name, table_name, value):
    df = context.spark.sql("select {0} from {1}".format(field_name, table_name))
    x = df.groupBy().min().collect()[0][0]
    assert (x == int(value))


@then(u'the max of field "{field_name}" in table "{table_name}" is "{value}"')
def step_impl(context, field_name, table_name, value):
    df = context.spark.sql("select {0} from {1}".format(field_name, table_name))
    x = df.groupBy().max().collect()[0][0]
    assert (x == int(value))


@then(u'the table "{table_name}" has "{row_count}" rows')
def step_impl(context, table_name, row_count):
    df = context.spark.sql("select * from {0}".format(table_name))
    assert (df.count() == int(row_count))


@then(u'the table "{table_name}" has "{col_count}" columns')
def step_impl(context, table_name, col_count):
    df = context.spark.sql("select * from {0}".format(table_name))
    assert (len(df.schema) == int(col_count))


@when(u'I execute the following SQL into table "{table_name}"')
def step_impl(context, table_name):
    df = context.spark.sql(context.text)
    df.createOrReplaceTempView(table_name)
