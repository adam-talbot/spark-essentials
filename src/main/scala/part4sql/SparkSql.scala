package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSql extends App {

  // start Spark session
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse") // set custom directory for sql dbs
    //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") // allow overwriting tables
    .getOrCreate()

  // read in carsDF
  val carsDF = spark.read
    .json("src/main/resources/data/cars.json")

  // regular df api
  carsDF.select(col("Name"), col("Origin")).where(col("Origin") === "USA")
    //.show(false)

  // do the same thing using Spark SQL
  // create the TempView
  carsDF.createOrReplaceTempView("cars")
  // run same filtration as above using SQL
  spark.sql(
    """
      |select Name, Origin from cars where Origin = 'USA'
      |""".stripMargin
  )//.show(false)
  // have full power of SQL using this syntax

  // can also create and edit tables
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  databasesDF//.show()

  // how to transfer from a DB to Spark SQL

  // create a method to read in tables
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  // read in one table
//  val employeesDF = readTable("employees")
//  employeesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("employees")

  // create method to read in list of tables from postgres
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach {
    tableName =>
      // create a Spark DF for each table
      val tableDF = readTable(tableName)
      // create Spark SQL temp view for each DF
      tableDF.createOrReplaceTempView(tableName)
      if (shouldWriteToWarehouse) {
        // write to Spark SQL DB
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
  }

  // now call the transferTables method
  transferTables(List("employees", "departments", "titles", "dept_emp", "dept_manager", "salaries", "movies"))

  // read a DF from loaded df
//  val employeesDF2 = spark.read.table("employees")
//  employeesDF2.show()

  val tablesDF = spark.sql("show tables")
  tablesDF.show()

  /**
    * EXERCISES:
    * 1. Read in movies DF and store it as Spark table in rtjvm DB (already done)
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
    * 3. Show the average salaries for the employees hired in between those dates, grouped by dept.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // 2
  spark.sql(
    """
      |SELECT count(*)
      |FROM employees
      |WHERE hire_date BETWEEN '1999-01-01' AND '2000-01-01';
      |""".stripMargin
  ).show()
  // not getting same results as him, not sure why

  // 3
  spark.sql(
    """
      |SELECT dept_name AS department, avg(salary) as avg_salary
      |FROM employees AS e
      |JOIN dept_emp AS de USING(emp_no)
      |JOIN departments AS d USING(dept_no)
      |JOIN salaries AS s using(emp_no)
      |WHERE hire_date BETWEEN '1999-01-01' AND '2000-01-01'
      |GROUP BY dept_name;
      |""".stripMargin
  )//.show()

  // alternate way using where to join
  spark.sql(
    """
      |SELECT dept_name AS department, avg(salary) as avg_salary
      |FROM employees AS e, dept_emp AS de, departments AS d, salaries AS s
      |WHERE hire_date BETWEEN '1999-01-01' AND '2000-01-01'
      | AND e.emp_no = de.emp_no
      | AND de.dept_no = d.dept_no
      | AND e.emp_no = s.emp_no
      |GROUP BY dept_name;
      |""".stripMargin
  )//.show()

  // 4
  spark.sql(
    """
      |SELECT dept_name AS department, avg(salary) as avg_salary
      |FROM employees AS e, dept_emp AS de, departments AS d, salaries AS s
      |WHERE hire_date BETWEEN '1999-01-01' AND '2000-01-01'
      | AND e.emp_no = de.emp_no
      | AND de.dept_no = d.dept_no
      | AND e.emp_no = s.emp_no
      |GROUP BY dept_name
      |ORDER BY avg_salary DESC
      |LIMIT 1;
      |""".stripMargin
  ).show()

}
