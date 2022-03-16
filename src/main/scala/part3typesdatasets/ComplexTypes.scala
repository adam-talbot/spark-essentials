package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    // had to add this due to having newer version of Scala
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  // read in movies df
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // dates
  // operate on dates when they stay as strings
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // get current date
    .withColumn("Right_Now", current_timestamp()) // get current timestamp
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // get diff in years
    // date_add and date_sub are also available
    // NOTE: when dates only have 2 digits for year, it will choose closest data (e.g. 2020 vs 1920) and this might cause issues in data
    // check to see if there are any negative values for Movie_age
    // .filter(col("Movie_Age") < 0) // there are several movies that are being parsed incorrectly due to this
    //.show(false)

  // if date parser fails, that col with be null for that row
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)//.show()
  // date format for these records is different

  /*
  EXERCISE:
  1. How do we deal with multiple date formats in the same df?
  If else logic to test for different formats and parse accordingly?
  Parse the DF multiple times, then union the small dfs
  Ignore dates in incorrect format if DF too large
  2. Read the stocks DF and parse the dates
   */

  // 2
  // read in stocks df
  val stocksDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select(col("symbol"), to_date(col("date"), "MMM dd yyyy").as("Date"))
    //.show()

  // Structures = groups of columns aggregated into one
  // V1 - with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    // select an entry from the struct col
    .select(col("Title"), col("Profit").getField(("US_Gross")).as("US_Profit"))
    //.show()
  // V2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
    //.show()

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  // use some functions on the column containing the array
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]").as("First_Word"), // get first entry in array
    size(col("Title_Words")).as("Word_Count"),
    array_contains(col("Title_Words"), "Love").as("Contains_Love")
  )//.filter("Contains_Love")
    //.show()

}
