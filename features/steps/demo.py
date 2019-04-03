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
