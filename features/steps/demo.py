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
    strong_beers = context.spark.sql("""
        select * from beers where abv > 5
    """)
    strong_beers.createOrReplaceTempView("strong_beers")

    grouped = context.spark.sql("""
        select b.name as beer_name, c.name as channel_name, s.hour as hour, sum(quantity) as sale_count 
        from sales s
        join channels c on (s.channel_id = c.id)
        join strong_beers b on (s.beer_id = b.id)
        group by hour, beer_name, channel_name
    """)
    grouped.createOrReplaceTempView("grouped")

    df = context.spark.sql("""
        select * from grouped where channel_name = 'Shop' order by hour asc
    """)
    df.createOrReplaceTempView(table_name)
    df.show()
