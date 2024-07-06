package challenge

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.ResourceBundle

object Part1 {

  def run(ss: SparkSession): DataFrame = {
    val rs = Funcs.getOrCreateProps()

    // read googleplaystore_user_reviews.csv
    val userReviewsFp = rs.getString("USER_REVIEWS_FP")
    var dfUserReviews = ss.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(userReviewsFp)

    // cast Sentiment_Polarity column to Double and convert any Nan to NULL
    // this will make it possible to ignore any NaN when calculating the average
    dfUserReviews = dfUserReviews
      .withColumn("Sentiment_Polarity", dfUserReviews("Sentiment_Polarity")
        .cast(DoubleType))

    // convert any NaN to null
    dfUserReviews = dfUserReviews
      .withColumn("Sentiment_Polarity",
        when(isnan(dfUserReviews("Sentiment_Polarity")), null).otherwise(dfUserReviews("Sentiment_Polarity")))

    // group rows by App and calculate average Sentiment_Polarity
    var df_1 = dfUserReviews.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    // convert any nulls to 0
    df_1 = df_1.na.fill(0)

    df_1
  }

  def main(args: Array[String]): Unit = {

    // create spark session
    val ss = spark.sql.SparkSession
      .builder()
      .appName("Part1")
      .master("local") // single threaded
      .getOrCreate()

    try {
      var df_1 = run(ss)
    } finally {
      ss.close()
    }

  }

}