package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  // read in movies df
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // select the first non-null value out of a series of cols
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Rating")
  )//.show()

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)
    //.show()

  // nulls when ordering, can put nulls at end or front of sort
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
    //.show()

  // removing rows where a specific column has null value
  moviesDF.select("Title", "IMDB_Rating").na.drop()
//  println(moviesDF.count())
//  println(moviesDF.select("Title", "IMDB_Rating").na.drop().count())
  // rows were dropped

  // .na object has various methods that can be used for imputation
  // replace nulls with fill() for a list of cols
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
    .select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating")
    //.show()

  // replace nulls with fill using different values for each column (map as param for fill()
  val fillMap = Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  )
  moviesDF.na.fill(fillMap)
    .select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating", "Director")
    //.show()

  // complex operations (using Spark SQL API instead of DF API)
  val complexNullOpsDF = moviesDF.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce above
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // alternate syntax for above
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  )
  // lets see if there are any cases where the two rating are equal
  complexNullOpsDF.filter(col("nullif").isNull && col("Rotten_Tomatoes_Rating").isNotNull)
    //.show()
  // take a look at results from complex ops
  complexNullOpsDF//.show()

}
