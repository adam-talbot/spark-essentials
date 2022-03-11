package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // read in DFs
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")
  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.json("src/main/resources/data/guitarPlayers.json")

  // let's do a join
  // it's best practice to pull out join condition and save as val
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  // inner join only takes rows where there is a logical match for join condition, no match, no row in joined df
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF//.show()

  // outer joins
  // left outer - contains everything from inner join (matches), all rows in the left table with nulls where match is missing in right table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")//.show()
  // right outer - inverse of left
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")//.show()
  // full outer
  guitaristsDF.join(bandsDF, joinCondition, "outer")//.show()

  // semi-joins
  // left semi - similar to inner join, only keeps matches, but doesn't pull in data from right table (same # of rows as inner, less columns)
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")//.show()

  // anti join - keeps rows in left df for which there is no match in the right df
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")//.show()

  // Things to keep in mind when joining:

  //guitaristsBandsDF.select("id").show() // this produces and exception since there are now two columns called "id"
  // how to solve:
  // 1 - rename the column before the join
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")//.show()
  // this will produce df with only one band column and one id column
  // 2 - drop the dup column after join
  guitaristsBandsDF.drop(bandsDF.col("id"))//.show()
  // have to reference column from original df before join to avoid same ambiguity
  // this works because the unique identifier associated with the column stays the same throughout the join
  // 3 - rename offending column before join and keep the redundant data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))//.show()
  // this keeps everything and makes each column unique
  // 4 - join and then rename dup column after join
  // guitaristsBandsDF.withColumn("bandId", bandsDF.col("bandId")).show
  // could not get this to work

  // using complex types
  // rename "id" column to avoid ambiguity, use expression to join
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))//.show()
  // this expression takes the guitars array from the guitaristsDF and checks each item to see if it matches guitarId from guitars df, if it matches,
  // it adds a column to the joined df for that guitar (we are exploding in a sense and adding more columns)

  /*
  EXERCISES
  1. Show all employees and their max salary
  2. Show all employees who were never managers
  3. Final all job titles of the best paid 10 employees in the company
   */

  // create a method to read in tables
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  // 1
  // read in employees df from db, read in salaries from db, join on emp_no, group by emp_no and display max sal
  val employeesDF = readTable("employees")

  //employeesDF.show(5)

  val salariesDF = readTable("salaries")

  //salariesDF.show(5)

  //val maxSalariesDF = salariesDF.groupBy("emp_no").max("salary")//.show()
  val maxSalariesDF = salariesDF.groupBy("emp_no").agg(
    functions.max("salary").as("Max_Salary")
  )

  employeesDF.join(maxSalariesDF, "emp_no")//.show()

  // 2
  // read in dept manager, use anti join to get all rows from employees that don't have match in dept manager
  val deptManagerDF = readTable("dept_manager")
  employeesDF.join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "left_anti")//.show()

  // 3 - read in titles df, get emp_nos for top 10 earners from max, current salary, join to titles using emp_no
  val titlesDF = readTable("titles")
  val top10SalariesDF = salariesDF.filter("to_date = '9999-01-01'").sort(col("salary").desc).limit(10)
  // can also do this using groupby and max but this will get max salary, not current salary, just depends what they are looking for
  //val top10Salaries = salariesDF.groupBy("emp_no", "salary").agg(max("to_date")).sort(col("salary").desc).limit(10)
  val currentTitlesDF = titlesDF.filter("to_date = '9999-01-01'")
  currentTitlesDF.join(top10SalariesDF, "emp_no")//.show()
}
