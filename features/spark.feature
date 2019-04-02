Feature: Some basic PySpark examples

  Scenario: Count some words with Spark
    Given a spark session
    When I count the words in "the complete works of Shakespeare"
    Then the number of words is '5'

  Scenario: Simple filter on a table
    Given a spark session
    And a table called "students" containing
      | name:String | age:Int |
      | Fred        | 9       |
      | Mary        | 10      |
      | Bob         | 10      |
    When I select rows from "students" where "age" greater than "10" into table "results"
    Then the table "results" contains
      | name:String | age:Int |
      | Mary        | 10      |
      | Bob         | 10      |

  Scenario: Simple join of two tables
    Given a spark session
    And a table called "students" containing
      | id:Int | name:String | subject_id:Int |
      | 1      | Fred        | 1              |
      | 2      | Sally       | 1              |
      | 3      | Susan       | 2              |
    And a table called "subjects" containing
      | id:Int | name:String |
      | 1      | Maths       |
      | 2      | Physics     |
    When I do the join business logic
    Then the table "results" contains
      | id:Int | student_name:String | subject_name:String |
      | 1      | Fred                | Maths               |
      | 2      | Sally               | Maths               |
      | 3      | Susan               | Physics             |

