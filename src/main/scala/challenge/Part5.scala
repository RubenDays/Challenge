package challenge

import org.apache.spark.sql.functions.{array_distinct, collect_set, count, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.ResourceBundle

object Part5 {

  def run(ss: SparkSession): DataFrame = {
    var df_4 = Part4.run(ss)

    // flatten the genres, so each row has only one genre. tuples will duplicate (in exception on genre)
    df_4 = df_4.withColumn("Genre", explode(df_4("Genres")))

    // group by the newly create column and aggregate it by counting number of apps, the average rating and the average
    // sentiment polarity for each genre.
    df_4 = df_4
      .groupBy(df_4("Genre"))
      .agg(Map(
        "App" -> "count",
        "Rating" -> "avg",
        "Average_Sentiment_Polarity" -> "avg",
      ))

    // rename the created columns
    df_4 = df_4.withColumnsRenamed(Map(
      "count(App)" -> "Count",
      "avg(Rating)" -> "Average_Rating",
      "avg(Average_Sentiment_Polarity)" -> "Average_Sentiment_Polarity",
    ))

    df_4
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Part5")
      .master("local")
      .getOrCreate()

    try {
      val df = run(ss)

      val rs = ResourceBundle.getBundle("props")
      val fn = rs.getString("APP_METRICS")
      val fnTmp = fn + "tmp"

      df.write
        .mode("overwrite")
        .option("header", "true")
        .option("compression", "gzip")
        .parquet(fnTmp)

      Funcs.renameFile(fn, fnTmp, ".parquet")
    } finally {
      ss.close()
    }

  }

}
