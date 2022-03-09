package part2dataframes

import org.apache.spark.sql.SparkSession
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
  spark.read
    .format("json")
    // in order to parse dates, must enforce a schema
    .schema(carsSchema)
    // parse a date format
    .option("dateFormat", "yyyy-MM-dd")

}
