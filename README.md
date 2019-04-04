# Spark + Python + Gherkin

This repo is an example of how you can wrap PySpark scripts in BDD, and how you can use Gherkin to bring less technical
people along for the ride.

## Why PySpark?

PySpark is great!  If you're a developer looking at this page, maybe go and have a look before cloning the repo ;)

If you're just generally interested, PySpark provides a means to create and deploy simple, batch data processing 
jobs _quickly and cheaply_. As a data engineer, sometimes you need a heavy-weight solution with Spark on pure JVM 
(Scala, Java etc) but other times you just need to develop an ETL quickly.

- Perhaps to extract data from a data lake for reporting: merging a day's worth of purchases into a finance report. 
- Perhaps to create models or summaries from large datasets: aggregating millions of rows of transactions 
into a daily summary view.  
- Maybe for training a machine learning model or keeping your feature store up to date.
- You might want to throw these jobs into Airflow or Luigi for the ultimate ETL zen

Building jobs quickly is great - you get your data from one place to another really quick. The downside is that it can
feel a bit hacky, which is where BDD/Gherkin comes in.

## Why Gherkin?

Behaviour Driven Development is pretty ubiquitous these days.  For the uninitiated, BDD is about testing the 
_behaviour_ of a process or app, rather than it's internal workings.  While TDD might test across the boundaries of a 
class or function, BDD focuses on inputs and outputs of a whole application - testing all the code as it does so.
BDD is less brittle than TDD (you can change implementation without rewriting your tests) and tends to be more closely related
to the acceptance criteria (given... when... then's) on your story cards.

Gherkin is a human readable language for BDD - it abstracts code away behind almost-natural-language statements.
These map to code which can be run just like unit tests, giving you instant feedback on whether your app's behaviour 
meets the spec.  Many developers see Gherkin as being 'old hat' these days - needless
syntactic-sugar which makes definition of tests slower and more cumbersome.  I happen to agree
with them... BUT I also feel that Gherkin has some advantages in the data world that are less obvious to people 
writing '_normal_' software:

- Gherkin is great for defining tables of data for tests
- Gherkin is accessible to non-coders - data analysts, BAs and suchlike

**Define a Scenario:**
```gherkin
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
```

**Run the 'tests', get feedback:**
```bash
$ behave
...
2 features passed, 0 failed, 0 skipped
9 scenarios passed, 0 failed, 0 skipped
41 steps passed, 0 failed, 0 skipped, 0 undefined
Took 0m25.693s
```

Data engineers are often tasked with writing code to transform data for reporting and analytics. However it's often 
others in the business who have the domain knowledge to define how the transformation should work - the specific
joins, filters and aggregations can often be pretty complex.  The expert might define the logic in a user story 
or discuss the task with an engineer round a whiteboard - but there's still much scope for misinterpretation and confusion.

This is where Gherkin is useful - it allows the Expert to define what they want in an unambiguous way and gives the
Engineer confidence that their code is doing the right thing.

# The Code

The following is a brief walk-through of the code.  I'm using the `behave` and `pyspark` libraries. Here's a good tip
for [making behave work in the community edition of IntelliJ](https://aditamasblog.wordpress.com/2018/07/26/setting-up-pycharm-community-edition-to-work-with-python-behave/).

In most cases I am using temporary tables in Spark's SqlContext to pass data around between steps.  This means the business logic
can be separated from the read and write operations without the need to create mocks.  In your BDD steps you throw the
data into tables with a give names, your code runs and creates more tables which you can test.  In your production app
you read the data from whatever source you have and throw it into the same set of tables - no mocks, class hierarchies
or other nastiness.

<img src="https://logicalgenetics.com/wp-content/uploads/2019/04/BDD-Spark.png" alt="What? No mocks?" width="600"/>

## Simple Filters and Joins

Examples of simple filters and joins can be found in `spark.feature`.  Implementation of the steps is spread across the
various .py files in the `features/steps` directory.

Here's an example Scenario a simple join:

```gherkin
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
```

The input tables are parsed and converted to named tables in the SqlContext like this:
```python
@given(u'a table called "{table_name}" containing')
def step_impl(context, table_name):
    df = table_to_spark(context.spark, context.table)
    df.createOrReplaceTempView(table_name)
```

The code under test, which is executed behind the `When I do the join business logic` step is pretty simple. Note that
data is passed in and out based on named tables only. This convention-over-configuration approach makes the transform logic very
easy to decouple from the read (extract) and write (load) operations.
```python
@when(u'I do the join business logic')
def step_impl(context):
    df = context.spark.sql("""
        select s.id, s.name as student_name, c.name as subject_name
        from students s
        join subjects c on (s.subject_id == c.id)
    """)
    df.createOrReplaceTempView("results")
```

## Automatic Data Generation

Writing Gherkin tables is boring, there's no two ways about it. Plus, sometimes you just need some rows - you don't want
to dream up fake names and addresses for your test 'customer' table.  So I wrote a few handy utilities for generating 
data automatically.

### Wildcards

Here we want to test the age filtering, but don't care about the names of the students.
```gherkin
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
```

### Random Rows

Here we just want 10 rows with a given schema.  Random values are inserted into every cell.
```gherkin
  Scenario: Random records
    Given a spark session
    And a table called "random_students" containing "10" rows with schema
      | name       | type   | mode |
      | id         | int    | RAND |
      | name       | string | RAND |
      | subject_id | long   | RAND |
    Then the table "random_students" has "10" rows
    And the table "random_students" has "3" columns
```

### Sequences

Particularly useful for ids.  Here we just want some rows with a sequential integer id:
```gherkin
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
```

Sequences are also possible with strings. English words are used for sequence values ('one', 'two', 'three'...).
```gherkin
  Scenario: String based sequences
    Given a spark session
    And a table called "string_table" containing "1000" rows with schema
      | name       | type   | mode |
      | name       | string | SEQ  |
    Then the table "string_table" has "1000" rows
    And the value "one" is present in the field "name" of table "string_table"
    And the value "one thousand" is present in the field "name" of table "string_table"
    And the value "thirty-six" is present in the field "name" of table "string_table"
```

### Slightly More Complex Example

Coming soon!
