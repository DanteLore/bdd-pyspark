
def customer_state(spark):
    state_updates = spark.sql("""
        select customer_id, state
        from (select
                customer_id,
                state,
                dense_rank() over (partition by customer_id ORDER BY ts DESC) as rank 
            from transactions) tmp
         where rank = 1
    """)
    state_updates.createOrReplaceTempView("state_updates")

    df = spark.sql("""
        select c.customer_id, coalesce(u.state, c.state) as state
        from customers c 
        left outer join state_updates u on (c.customer_id = u.customer_id)
    """)

    df.show()
    return df
