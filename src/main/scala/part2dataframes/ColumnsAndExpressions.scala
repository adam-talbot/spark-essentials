package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  // get some data to work with
  val carsDF = spark.read
    .json("src/main/resources/data/cars.json")

  // create a column object
  val firstColumn = carsDF.col("Name")
  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  // selecting using columns/expressions
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // interpolated string
    expr("Origin") // EXPRESSION
  ).show()

  // do this in simpler fashion just using list of strings
  carsDF.select("Name", "Acceleration", "Year").show()
  // can with pass in col objects or strings but must be consistent, can't mix and match

  // EXPRESSIONS
  // simplest expression is to just the column object
  val simplestExpression = carsDF.col("Weight_in_lbs")
  // perform operation on whole column
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  // select some col objects into a new DF including the expression
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    // simpler way to do this using expr
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // many times in practice there are a bunch of expr() calls in the select statement
  // selectExpr was developed to simplify this
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2" // not sure if you can alias with this approach
  )

  carsWithWeightsDF.show()
  carsWithSelectExprWeightsDF.show()

  // DF processing
  // add new column withColumn(new_column_name, expression)
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // when using expr, spaces can cause issues so you need to use backticks to escape them
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show() // otherwise the spaces here would break the compiler
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement").show()

  // FILTERING
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA") // must use this operator as to not interfere with standard scala operator
  // would be === for ==
  // .where() does the same thing
  //europeanCarsDF.show()

  // using expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'") // use single quotes inside of expression
  //americanCarsDF.show()
  // chain filters
  val americanPowerfulCars = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  //americanPowerfulCars.show()
  // chain them using and
  val americanPowerfulCars2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  //val americanPowerfulCars3 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150)) // without using in fix notation

  // using expressions SIMPLEST WAY
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") // use single quotes inside of expression
  //americanPowerfulCarsDF3.show()

  // unioning - adding more rows
  // read in a new json with 2 more rows of car data
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")
  // join them
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // test this
  println(s"Rows in carsDF: ${carsDF.count()}")
  println(s"Rows in allCarsDF: ${allCarsDF.count()}")

  // distinct - get number of distict values in a column
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()

  /*
  Exercises
  1. Read in the movies df and select 2 columns
  2. Create another column summing up total profit (us_gross + worldwide_gross + dvd_sales)
  3. Select all comedy movies with IMDB rating above 6

  For all exercises, use as many versions as possible
   */

  // 1
  // read in the movies df
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  // select 2 columns
  moviesDF.select("Title", "US_Gross").show()
  // could also select using column objects using various methods shown above
  // could also use selectExpr()

  // 2
  // using expression
  moviesDF.withColumn("Total Profit", expr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")).show()
  // alternate method
  moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    //(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"),
    // remove dvd sales since they are mostly null values
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )//.show()

  // use selectExpr
  moviesDF.selectExpr(
    "Title",
    // other relevant columns
    "US_Gross + Worldwide_Gross as Total_Gross"
  )//.show()

  // 3
  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").select("Major_Genre", "IMDB_Rating")//.show()
  // do not need to use expr() here, automatically looks for expression
  
}

