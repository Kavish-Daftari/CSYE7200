package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object SparkMovieRatings {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Movie Ratings Analysis")
      .config("spark.master", "local")
      .getOrCreate()


    val filePath = "/Users/kavishdaftari/Bigdata/CSYE7200/spark-csv/src/main/resources/movie_metadata.csv" // Update this path

    val df = spark.read.option("header", "true").csv(filePath)
    df.show()

    val processedDf = calculateStatistics(df)
    processedDf.show()
  }

  def calculateStatistics(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    df.agg(
        mean("imdb_score").alias("mean_imdb_score"),
        stddev("imdb_score").alias("std_dev_imdb_score")
      )
  }
}