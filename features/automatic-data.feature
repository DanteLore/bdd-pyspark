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
