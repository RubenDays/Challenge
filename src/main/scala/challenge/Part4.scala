package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.ResourceBundle

object Part4 {

  def run(ss: SparkSession): DataFrame = {
    val df_1 = Part1.run(ss)
    val df_3 = Part3.run(ss)

    // join df_1 and df_3 by App
    // Note that there are different databases (files) that contains different Apps, so the inner join will be smaller
    // than any of them
    val df = df_1.join(df_3, Seq("App"))

    df
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Part4")
      .master("local")
      .getOrCreate()

    try {
      val df = run(ss)

      val rs = ResourceBundle.getBundle("props")
      val fn = rs.getString("APP_CLEANED")
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
