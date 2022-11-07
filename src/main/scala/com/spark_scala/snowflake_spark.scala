package com.spark_scala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}
object snowflake_spark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDemo04")
      .getOrCreate()

    val sfOptions = Map(
      "sfURL" -> "https://jmb86101.us-east-1.snowflakecomputing.com",
      "sfUser" -> "Sunanda",
      "sfPassword" -> "Sunanda1993@",
      "sfDatabase" -> "Test_db",
      "sfSchema" -> "Test_schema",
      "sfWarehouse" -> "Test_WH",
      "sfRole" -> "accountadmin"
    ) 

    val df: DataFrame = spark.read
      .format("net.snowflake.spark.snowflake") // or just use "snowflake"
      .options(sfOptions)
      .option("dbtable", "big_data_table1")
      //.option("query", "select id, name, preferences, created_at from BIG_DATA_TABLE1")
      .load()

    df.show(false)
  }
}






