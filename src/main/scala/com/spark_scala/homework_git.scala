package com.spark_scala
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Column
import net.snowflake.spark.snowflake.Utils.{SNOWFLAKE_SOURCE_NAME, classForName}
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.catalyst.ScalaReflection.universe.Select
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col,length}
import scala.language.postfixOps
import org.apache.spark.sql.expressions._

object homework_git {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDemo04")
      .getOrCreate()

    //read json file into dataframe
    var df = spark.read.json("C:\\Users\\Consultant\\Downloads\\Votes.json")
    df.show()
    var df2 = df.select(weekofyear(col("CreationDate")).alias("weekpervotes"),year(col( "CreationDate"))
      .alias("yearofvotes"), col(colName = "Id"))


    val df3 = df2.groupBy(col("weekpervotes"),col(colName = "yearofvotes")).count().sort("yearofvotes").withColumn("percentage", col("count") / sum("count").over())

    df3.withColumn(colName = "weekoutlier",(when(col("percentage")>0.002,"outlier")).otherwise("nooutlier"))
    df3.show()

    //df3.write.csv("/tmp/sample1")

     val sfOptions = Map(
      "sfURL" -> "https://qgb20383.us-east-1.snowflakecomputing.com",
      "sfUser" -> "sunanda1959",
      "sfPassword" -> "Sunanda1993@",
      "sfDatabase" -> "TEST_DB1",
      "sfSchema" -> "SCHEMA1",
      "sfWarehouse" -> "TEST_WH",
      "sfRole" -> "accountadmin"
    )
    val df1:Unit=df3.write
        .format("net.snowflake.spark.snowflake") // or just use "snowflake"
        .options(sfOptions)
        .option("dbtable", "JSON_TABLEE1")
      //.option("query", "select id, name, preferences, created_at from BIG_DATA_TABLE1")
        .mode(SaveMode.Overwrite)
        .save()
  }


}


