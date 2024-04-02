package edu.neu.coe.csye7200.csv


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieRatingsAnalysisTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingAnalysisTest")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Define the test DataFrame
  val testDF: DataFrame = spark.createDataFrame(Seq(
    ("Movie1", 8.0),
    ("Movie1", 7.5),
    ("Movie2", 6.5),
    ("Movie3", 9.0),
    ("Movie3", 8.5)
  )).toDF("movie_title", "imdb_score")

  // Test for expected columns in the result DataFrame
  "calculateStatistics method" should "return DataFrame with expected columns" in {
    val resultDF = SparkMovieRatings.calculateStatistics(testDF)
    resultDF.columns should contain theSameElementsAs Seq("mean_imdb_score", "std_dev_imdb_score")
  }

  // Test for expected number of rows (should always be 1)
  it should "return DataFrame with exactly one row" in {
    val resultDF = SparkMovieRatings.calculateStatistics(testDF)
    resultDF.count() shouldEqual 1
  }

  // Test for accuracy of the calculated values
  it should "calculate the correct global mean and standard deviation of imdb_score" in {
    import spark.implicits._
    // Assuming imdb_score is of type Double. Adjust if it's stored as String or another type.
    val resultDF = SparkMovieRatings.calculateStatistics(testDF)
    val (meanImdbScore, stdDevImdbScore) = resultDF.as[(Double, Double)].collect().head

    meanImdbScore should be (7.9 +- 0.1) // Adjust tolerance as needed
    stdDevImdbScore should be (1.0 +- 0.1) // Adjust tolerance as needed, calculate expected std dev if necessary
  }

}


