package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    // create a new app name for each sesssion
    .appName("DataFrames Basics")
    // can call config multiple times specifying different param each time
    .config("spark.master", "local")
    // always end with this
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // show first 5 rows of DF
  firstDF.show(5)
  // DF is a distributed collection of row objects conforming to the schema

  // take a look at schema
  firstDF.printSchema()

  // take first 10 rows and print each
  firstDF.take(10).foreach(println)
  // every row is an array of something

  // spark types are case objects
  // known at run time instead of compile time
  // each is a singleton
  val longType = LongType

  // define a schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // take a look at schema for existing df
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read a DF with a defined schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow =Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  manualCarsDF.show()

  // NOTE: DFS HAVE SCHEMAS, ROWS DO NOT

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  // print the schemas for both DFs to see the difference in these two methods of creating DFs from tuples
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /**
    * EXERCISES:
    * 1. create a new df manually describing smart phones
    *   - make, model, screen dimension, camera megapixels, etc.
    * 2. Read another file from data folder, print schema, and count number of rows
   */

  // exercise 1
  val phones = Seq(
    ("Apple","iPhone X","500x600",1000),
    ("Samsung","Galaxy Note","500x700",2000)
  )
  val manualPhonesDFWithImplicits = phones.toDF("Make", "Model", "ScreenDims", "CameraMP")
  manualPhonesDFWithImplicits.printSchema()
  manualPhonesDFWithImplicits.show(5)

  // exercise 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.show(5)
  moviesDF.printSchema()
  println(moviesDF.count())

}
