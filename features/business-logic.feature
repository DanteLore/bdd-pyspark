Feature: Realistic business logic examples

#  Scenario: Three way join and filter
#    Given a spark session
#    And a table called "beers" containing
#      | id:int | name:string | abv:double |
#      | 1      | Weak Beer   | 3.2        |
#      | 2      | Strong Beer | 6.4        |
#      | 3      | Medium Beer | 4.6        |
#    And a table called "channels" containing
#      | id"int | name:string |
#      | 1      | Web         |
#      | 2      | Shop        |
#      | 3      | Pub         |
#    And a table called "sales" containing "1000" rows with schema
#      | name       | type   | mode       |
#      | beer_id    | int    | RAND |
#      | channel_id | string | RAND |
#      | quantity   | double | RAND |
