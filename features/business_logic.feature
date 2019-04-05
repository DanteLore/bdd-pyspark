Feature: Realistic business logic examples

  Scenario: Three way join and filter: Hourly report on sales volume of strong beers in shops
    Given a spark session
    And a table called "beers" containing
      | id:int | name:string | abv:double |
      | 1      | Weak Beer   | 3.2        |
      | 2      | Strong Beer | 6.4        |
      | 3      | Medium Beer | 4.6        |
    And a table called "channels" containing
      | id:int | name:string |
      | 1      | Web         |
      | 2      | Shop        |
      | 3      | Pub         |
    And a table called "sales" containing "1000" rows with schema
      | name       | type   | mode       |
      | id         | int    | SEQ        |
      | beer_id    | int    | RAND(1-3)  |
      | channel_id | int    | RAND(1-3)  |
      | quantity   | int    | RAND(1-5)  |
      | hour       | int    | RAND(0-23) |
    When I generate a summary in table "my_summary" of beers over 5 percent, sold in the shop by hour
    And I execute the following SQL into table "test_results"
    """
    select * from my_summary where channel_name != "Shop" or beer_name != "Strong Beer"
    """
    Then the table "my_summary" is not empty
    And the table "test_results" has "0" rows


  Scenario: Events to models: State changes out of order
    Given a spark session
    And a table called "customers" containing
      | customer_id:int | state:string |
      | 1               | Baz          |
      | 2               | Baz          |
      | 3               | Baz          |
    And a table called "transactions" containing
      | customer_id:int | ts:timestamp        | state:string |
      | 1               | 2019-01-01 00:00:10 | Foo          |
      | 2               | 2019-01-01 00:00:10 | Bar          |
      | 1               | 2019-01-01 00:00:09 | Foo          |
      | 1               | 2019-01-01 00:00:01 | Bar          |
    When I update my customers based on their recent transactions into table "updated_customers"
    Then the table "updated_customers" contains
      | customer_id:int | state:string |
      | 1               | Foo          |
      | 2               | Bar          |
      | 3               | Baz          |
