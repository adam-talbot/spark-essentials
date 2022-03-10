package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  // read in movies json
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // counting
  // get count of all values for column specified (except null)
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  //genresCountDF.show()
  // do same thing with expr()
  moviesDF.selectExpr("count(Major_Genre)")//.show()
  moviesDF.select(count("*"))//.show() // total rows in DF with
  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))//.show()
  // approximate versions - useful for very large dfs when only approximation is needed
  // count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))//.show()
  // min and max
  moviesDF.select(min(col("IMDB_Rating")))//.show()
  moviesDF.selectExpr("max(IMDB_Rating)")//.show()
  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")
  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  // can also do with selectExpr
  // can also use mean(), stddev()

  // Grouping
  moviesDF.groupBy(col("Major_Genre"))
    .count()//.show() // includes null
  moviesDF.groupBy("Major_Genre")
    .avg("IMDB_Rating")//.show()
  // use agg function to do multiple aggregations on same groups
  moviesDF.groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")
    //.show()

  /*
  Exercises:
  1. Sum up all profits for all movies
  2. Count how many distinct directors there are
  3. Show mean and standard deviation for US Gross Rev
  4. Compute the average IMDB rating and average US Gross Rev per director
   */

  // 1
  moviesDF.withColumn("Total_Gross", expr("US_Gross + Worldwide_Gross + US_DVD_Sales"))
    .select(sum("Total_Gross").as("Sum_of_Gross"))
    //.show()

  // 2
  moviesDF.select(countDistinct("Director").as("N_Directors"))//.show()

  // 3
  moviesDF.select(
    avg("US_Gross").as("Avg_US_Gross"),
    stddev("US_Gross").as("SD_US_Gross"))
    //.show()

  // 4
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    )
    //.orderBy(desc("Avg_US_Gross"))
    // more options when using it this way
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

}

