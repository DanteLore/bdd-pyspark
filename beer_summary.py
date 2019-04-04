
def beer_summary(spark):
    strong_beers = spark.sql("""
            select * from beers where abv > 5
        """)
    strong_beers.createOrReplaceTempView("strong_beers")

    grouped = spark.sql("""
            select b.name as beer_name, c.name as channel_name, s.hour as hour, sum(quantity) as sale_count 
            from sales s
            join channels c on (s.channel_id = c.id)
            join strong_beers b on (s.beer_id = b.id)
            group by hour, beer_name, channel_name
        """)
    grouped.createOrReplaceTempView("grouped")

    df = spark.sql("""
            select * from grouped where channel_name = 'Shop' order by hour asc
        """)
    df.show()
    return df