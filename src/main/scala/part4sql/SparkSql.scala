package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSql extends App {

  // start Spark session
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .getOrCreate()

  // read in carsDF
  val carsDF = spark.read
    .json("src/main/resources/data/cars.json")

  // regular df api
  carsDF.select(col("Name"), col("Origin")).where(col("Origin") === "USA")
    .show(false)

  // do the same thing using Spark SQL
  // create the TempView
  carsDF.createOrReplaceTempView("cars")
  // run same filtration as above using SQL
  spark.sql(
    """
      |select Name, Origin from cars where Origin = 'USA'
      |""".stripMargin
  ).show(false)




}
