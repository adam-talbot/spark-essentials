package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  // define a schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    //StructField("Year", StringType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  Reading a DF:
  - format (type of file)
  - schema or inferSchema = true
  - zero or more options
    - mode
      - tells spark what to do if a row does not conform to schema
  - path (can be on local, AWS path, etc.
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    //.option("inferSchema", "true")
    .option("mode", "failFast") // failFast (throw an exception for bad row), dropMalformed (drop incomplete or nonconforming rows), permissive (default)
    //.option("path", "src/main/resources/data/cars.json") // can also do this and then just call .load() at the end
    .load("src/main/resources/data/cars.json")

  // change a value in the json to be a different dtype and call an action to see how failFast works
  // change the value back

  // create an option map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
  Writing DFs:
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path
  - zero or more options
    - have options for sorting, bucketing, and partitioning (will cover later)
   */

  carsDF.write
    .format("json")
    .mode("overwrite")
    //.move(SaveMode.Overwrite) // more type-safe method of doing same as line above
    //.option("path", "src/main/resources/data/cars_dupe.json") // alternative to just adding path to save()
    .save("src/main/resources/data/cars_dupe.json")
  // This will have the json in a folder with the above name and actual json files within for each partition (if necessary)

  // json flags
  val carsDFWithFlags = spark.read
    //.format("json")
    // in order to parse dates, must enforce a schema
    .schema(carsSchema)
    // parse a date format
    .option("dateFormat", "yyyy-MM-dd") // if Spark fails parsing, will put NULL
    // can also use timestamp format if want more granularity than just day
    // if json formatted with single quotes, add this option
    .option("allowSingleQuotes", "true")
    // compression options for writing, spark can also read and extract these formats as well
    .option("compression", "uncompressed") // other options: bzip2, gzip, lz4, snappy (default), deflate
    //.load("src/main/resources/data/cars.json")
    // alternative to using both .format and .load
    .json("src/main/resources/data/cars.json")

  // check out results
  carsDFWithFlags.show(5)

  // csv flags - hard to parse in spark, has largest amount of flags for that reason, will go over most important here
  // create schema for stocks.csv
  val stocksSchema = StructType(Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    ))

  val stocksDFWithFlags = spark.read
    //.format("csv")
    .schema(stocksSchema)
    // observe the format of the dates from the csv file
    .option("dateFormat", "MMM d yyyy")
    // csv does have a header row
    .option("header", "true")
    // set the delimiter
    .option("sep", ",")
    // there are no notion of null values in csv, so this is important
    .option("nullValue", "")
    // many more options including: escaping commas in values, multiline, quotes, etc.
    //.load("src/main/resources/data/stocks.csv")
    .csv("src/main/resources/data/stocks.csv")

  // check results
  stocksDFWithFlags.show(5)

  // parquet - works very well with spark, open source, optimized for fast reading of columns
  // default format for dfs
  // very predictable, don't need as my flags
  carsDF.write
    //.format("parquet") // not needed since this is the default
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")
    // file in binary compressed, so is impossible to interpret in raw format
    // much less space than json, in practice up to 8-10x smaller

  // Text files
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt").show()
  // every line is written as a new row in the same column by default

  // reading from a remote DB
  // start postres database so that it is waiting for connections in terminal window
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show(5)

  /*
  Exercises:
  - read the movies df from the json
    - write it as:
      - tab-separated csv
      - snappy Parquet
      - table "public.movies" in Postgres DB
   */

  // read in movies df from json
  // just using defaults here and inferschema, this will not parse dates, but it fine for this exercise
  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  // write as tab separated csv
  moviesDF.write
    .format("csv")
    // csv does have a header row
    .option("header", "true")
    // set the delimiter
    .option("sep", "\t")
    // there are no notion of null values in csv, so this is important
    //.option("nullValue", "")
    .mode(SaveMode.Overwrite)
    // many more options including: escaping commas in values, multiline, quotes, etc.
    .save("src/main/resources/data/movies.csv")
    //.csv("src/main/resources/data/movies.csv")

  // write as snappy parquet
  moviesDF.write
    .save("src/main/resources/data/movies.parquet")

  // write to postgres db
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()

}
