from behave import *


@when('I count the words in "{text}"')
def step_impl(context, text):
    words = context.spark.sparkContext.parallelize(text.split(" "))

    context.result = words.count()


@then("the number of words is '{x}'")
def step_impl(context, x):
    assert (context.result == int(x))


@when(u'I do the join business logic')
def step_impl(context):
    df = context.spark.sql("""
        select s.id, s.name as student_name, c.name as subject_name
        from students s
        join subjects c on (s.subject_id == c.id)
    """)
    df.createOrReplaceTempView("results")


@when(u'I generate a summary in table "{table_name}" of beers over 5 percent, sold in the shop by hour')
def step_impl(context, table_name):
    from beer_summary import beer_summary
    df = beer_summary(context.spark)
    df.createOrReplaceTempView(table_name)


@when(u'I update my customers based on their recent transactions into table "{table_name}"')
def step_impl(context, table_name):
    from customer_state import customer_state
    df = customer_state(context.spark)
    df.createOrReplaceTempView(table_name)
