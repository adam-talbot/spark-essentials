package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  // start spark session
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // read in DFs
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")
  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.json("src/main/resources/data/guitarPlayers.json")

  // let's do a join
  // it's best practice to pull out join condition and save as val
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  // inner join only takes rows where there is a logical match for join condition, no match, no row in joined df
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()

  // outer joins
  // left outer - contains everything from inner join (matches), all rows in the left table with nulls where match is missing in right table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")//.show()
  // right outer - inverse of left
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")//.show()
  // full outer
  guitaristsDF.join(bandsDF, joinCondition, "outer")//.show()

  // semi-joins
  // left semi - similar to inner join, only keeps matches, but doesn't pull in data from right table (same # of rows as inner, less columns)
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti join
  // 


}
