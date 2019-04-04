Feature: Generation of datasets automatically

  Scenario: Random records
    Given a spark session
    And a table called "random_students" containing "10" rows with schema
      | name       | type   | mode |
      | id         | int    | RAND |
      | name       | string | RAND |
      | subject_id | long   | RAND |
    Then the table "random_students" has "10" rows
    And the table "random_students" has "3" columns

  Scenario: Semi-random records
    Given a spark session
    And a table called "students" containing
      | name:String | age:Int |
      | %RAND%      | 9       |
      | %RAND%      | 10      |
      | %RAND%      | 10      |
    When I select rows from "students" where "age" greater than "10" into table "results"
    Then the table "results" has "2" rows
    And the value "%RAND%" is not present in the field "name" of table "results"

  Scenario: Records with random integers
    Given a spark session
    And a table called "students" containing
      | name:String | age:Int |
      | Bruce       | %RAND%  |
      | Sandy       | %RAND%  |
      | Rajiv       | %RAND%  |
    Then the sum of field "age" in table "students" is greater than zero

  Scenario: Sequences
    Given a spark session
    And a table called "random_students" containing "3" rows with schema
      | name       | type   | mode |
      | id         | int    | SEQ  |
      | name       | string | RAND |
      | subject_id | long   | RAND |
    Then the min of field "id" in table "random_students" is "1"
    And the max of field "id" in table "random_students" is "3"
    And the sum of field "id" in table "random_students" is "6"

  Scenario: Sequences with lots of rows
    Given a spark session
    And a table called "big_table" containing "100000" rows with schema
      | name       | type   | mode |
      | big_id     | int    | SEQ  |
    Then the max of field "big_id" in table "big_table" is "100000"

  Scenario: String based sequences
    Given a spark session
    And a table called "string_table" containing "1000" rows with schema
      | name       | type   | mode |
      | name       | string | SEQ  |
    Then the table "string_table" has "1000" rows
    And the value "one" is present in the field "name" of table "string_table"
    And the value "one thousand" is present in the field "name" of table "string_table"
    And the value "thirty-six" is present in the field "name" of table "string_table"

  Scenario: Joining two auto-generated tables
    Given a spark session
    And a table called "widgets" containing "1000" rows with schema
      | name       | type   | mode |
      | id         | int    | SEQ  |
      | widget     | string | SEQ  |
    And a table called "macguffins" containing "1000" rows with schema
      | name       | type   | mode |
      | id         | int    | SEQ  |
      | macguffin  | string | SEQ  |
    When I execute the following SQL into table "joined_things"
    """
    select w.widget, m.macguffin
    from widgets w
    join macguffins m on (w.id = m.id)
    """
    And I execute the following SQL into table "filtered_joined_things"
    """
    select * from joined_things where widget = 'one thousand' and macguffin = 'one thousand'
    """
    Then the table "joined_things" has "1000" rows
    And the table "filtered_joined_things" has "1" rows

