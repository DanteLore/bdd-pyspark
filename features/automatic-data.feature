Feature: Generation of datasets automatically

  Scenario: Random records
    Given a spark session
    And a table called "random_students" containing "10" rows with schema
      | name       | type   |
      | id         | int    |
      | name       | string |
      | subject_id | long  |
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
      | %RAND%      | %RAND%  |
      | %RAND%      | %RAND%  |
      | %RAND%      | %RAND%  |
    Then the sum of field "age" in table "students" is greater than zero