package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  // start Spark session
  val spark = SparkSession.builder()
    .appName("RDDS")
    .config("spark.master", "local")
    .getOrCreate()

  // need the spark context to operate on RDDS
  val sc = spark.sparkContext

  // HOW TO CREATE RDD
  // 1. parallelize collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2. reading from files
  // case class first
  case class StockValue(symbol: String, date: String, price: Double)
  // define a method to read in values from csv line by line
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // drop header row
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  // call method and parallelize
  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // filter out header row using this logic
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  // convert into DS
  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd
  // this has the rdd with the correct type

  // can also covert directly from DF
  val stocksRDD4 = stocksDF.rdd
  // note: here you lose the typing and have rdd of type Row

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // lose type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // keep type info

  // TRANSFORMATIONS
  // filter
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  // count
  val msCount = msftRDD.count() // eager action
  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy
  // max
  // need implicit
  implicit val stockOrdering : Ordering[StockValue] = {
  Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  // added all possible locations to specify typing, only need one
  }
  val minMsft = msftRDD.min()// action
  // reduce
  numbersRDD.reduce(_ + _)
  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // expensive operation due to reshuffling

  // partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  // write to file to see what this does
  repartitionedStocksRDD.toDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stocks30")
  // repartitioning is expensive, involves reshuffling, partition early, then process that.
  // size of partition should be 10-100 MB

  // coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling, just moving to less partitions
  // could also save this to a file and see the 15 partitions

  /**
    *  EXERCISES
    *  1. Read the movies.json as RDD
    *  2. Show the distinct genres as RDD
    *  3. Select all movies from Drama genre with imdb rating > 6
    *  4. show average rating of movies by genre
    */

  // 1
  // case class
  case class Movie(title:String, genre: String, rating: Double)
  // read from a DF
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF.select(col("Title").as("title"),
    col("Major_Genre").as("genre"),
    col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val distinctGenresRDD = moviesRDD.map(_.genre).distinct() // also lazy
  distinctGenresRDD.toDF.show()

  // 3
  val goodMoviesRDD = moviesRDD.filter(_.genre == "Drama").filter(_.rating > 6) // lazy transformation
  goodMoviesRDD.toDF.show()

  // 4
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show()

}


