package part3typesdatasets

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{array, array_contains, avg, col, expr}

object Datasets extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // read in numbers df
  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true") // when I didn't include this option, it imported values as strings
    .csv("src/main/resources/data/numbers.csv")

  // check the type for the one column present
  numbersDF.printSchema()

  // Covert DF to DS when DF only has on column
  // have to define the encoder first
  implicit val intEncoder = Encoders.scalaInt
  // create the dataset so that it becomes a collection of JVM object instead of a collection of Spark row objects
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // Why do this? So we can use Scala syntax to work with DS directly instead of having to use DF API
  numbersDS.filter(_ < 100)
    //.show()
  // instead of
  numbersDF.filter(col("numbers") < 100)
    //.show()

  // Convert DF to DS when there are multiple cols
  // 1. define a case class
  case class Car(
                Name: String, // make sure all cases match column names exactly
                Miles_per_Gallon: Option[Double], // have to add option wherever there are nulls values in the DF
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String, // could also use Date type here, but diff Spark versions parse differently
                Origin: String
                )

  // create method to read in DF from json
  def readDF(filename: String) : DataFrame = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // 2. Read in DF
  val carsDF = readDF("cars.json")

  // 3. define an encoder (import the implicits)
  import spark.implicits._

  // 4. convert DF to DS
  val carsDS = carsDF.as[Car]

  // do things on the DS as you would on any other scala collection
  // map, flatMap, fold, reduce, for comprehensions, etc.
  carsDS.map(car => car.Name.toUpperCase())//.show()
  // let me try something else (doesn't work)
  //carsDS.map(_.Name => _.Name.toUpperCase())

  /**
    * Exercises
    * 1. count how many cars
    * 2. count how many cars have HP over 140
    * 3. find average HP
    */

  // 1
  println(carsDS.count())

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  // have to use getOrElse() because this col contains nulls
  // have to make else condition Long for it to compile

  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count())
  // can also use spark sql DF functions
  // this is because DFs are just DS of type row DS[Row]
  carsDS.select(avg(col("Horsepower"))).show()
  // number is slightly different, seems that in the DS the result is returned as an int causing rounding/truncation
  // at the final result and at intermediate steps

  // JOINING AND GROUPING DATASETS

  // create the DF
  val guitarsDF = readDF("guitars.json")
  val guitaristsDF = readDF("guitarPlayers.json")
  val bandsDF = readDF("bands.json")
  // create the case class
  case class Guitar(id: Long, make: String, model:String, guitarType: String)
  case class Guitarist(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)
  // convert DF to DS
  val guitarsDS = guitarsDF.as[Guitar]
  val guitaristsDS = guitaristsDF.as[Guitarist]
  val bandsDS = bandsDF.as[Band]

  // join guitarists with bands
  // .join() method will return a DF and lose types, so use joinWith
  val guitaristsBandsDS: Dataset[(Guitarist, Band)] = guitaristsDS.joinWith(bandsDS, guitaristsDS.col("band") === bandsDS.col("id"), "inner")
  guitaristsBandsDS//.show()
  // returns a dataset of tuples

  /*
  Exercise
  1. join the guitarsDS with guitaristsDS in an outer join (use array_contains)
   */
  // guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))//.show()
  val guitaristsGuitarsDS = guitaristsDS.joinWith(guitarsDS.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"), "outer")
    //.show()
  // without using expr
  guitaristsDS.joinWith(guitarsDS, array_contains(guitaristsDS.col("guitars"), guitarsDS.col("id")), "outer")
    //.show()

  // GROUPING DATASETS
  carsDS.groupByKey(_.Origin) // just use this notation to group on Origin col
    .count()
    .show()

  // JOINS AND GROUPS ARE WIDE TRANSFORMATIONS AND INVOLVE SHUFFLING (COMPUTATIONALLY EXPENSIVE)

  }
