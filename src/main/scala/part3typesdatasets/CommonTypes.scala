package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  // read in movies df
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // adding plain values using lit()
  moviesDF.select(col("Title"), lit(47))//.show()

  // Booleans
  // make some boolean masks/filters
  val dramaFilter = col("Major_Genre") equalTo "Drama" // could also use ===
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  // chain filters together
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title", "Major_Genre", "IMDB_Rating").where(preferredFilter)//.show()
  // there are multiple ways to use these filters, just some examples above

  // can also add the filters as booleans mask columns to the df
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))//.show()
  // and then use the boolean column to filter
  moviesWithGoodnessFlagsDF.filter("good_movie")//.show() // filters rows to only true
  // negations also possible
  moviesWithGoodnessFlagsDF.filter(not(col("good_movie")))//.show() // filters rows to only false

  // Numbers
  // check schema to see dtypes
  // moviesDF.printSchema()
  // math operators
  // find the normalized average rating
  moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)//.show()
  // correlation - number between -1 and 1
  //println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an action

  // read in cars df
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // string formatting
  carsDF.select(initcap(col("Name")))//.show()
  carsDF.select(upper(col("Name")))//.show()
  carsDF.select(lower(col("Name")))//.show()

  // contains
  carsDF.select("*").filter(col("Name").contains("volkswagen"))//.show()
  // regex
  // for filtering
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract") // will extract the pattern if present, if not will return ""
  ).where(col("regex_extract") =!= "") // filter to columns that have an extract
    .drop("regex_extract") // only used for filtering, not needed in final df
  vwDF.select(count("*").as("VW_Count"))//.show()
  // for replacing (to standardize category
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace") // will replace just pattern, and keep rest of string
  )//.show()

  /**
    * EXERCISES:
    * 1. Create a complex filter that takes in a list of names and filters based on that list
    */

  val carList = List("Volkswagen", "Mercedes-Benz", "Ford")
  //val complexRegex = "volkswagen|mercedes-benz|ford"
  // create the regex programmatically
  val complexRegex = carList.map(_.toLowerCase()).mkString("|")
  val filteredDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract") // will extract the pattern if present, if not will return ""
  ).where(col("regex_extract") =!= "") // filter to columns that have an extract
    //.drop("regex_extract") // only used for filtering, not needed in final df
    //.show()

  // try to solve using contains

  val cond1 = col("Name").contains("volkswagen")
  val cond2 = col("Name").contains("mercedes-benz")
  val cond3 = col("Name").contains("ford")
  val final_cond = cond1 or cond2 or cond3
  carsDF.select("Name").filter(final_cond).show()

  // solve programmatically with any list from API
  val carNameFilters = carList.map(_.toLowerCase())/*lower-case*/.map(name =>col("Name").contains(name)) //map to a list of filters similar to above
  // use fold to combine filters into big filter
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  // println(bigFilter) // how does this false filter affect results
  carsDF.filter(bigFilter).show()
}
