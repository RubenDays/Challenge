package challenge

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{desc, isnan}

object Part2 {

  def run(ss: SparkSession): DataFrame = {

    // extract properties
    val rs = Funcs.getOrCreateProps()

    // read googleplaystore.csv
    val appFp = rs.getString("APP_FP")
    val dfApp = ss.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(appFp)

    // filter data by rating > 4.0 and removes any NaN, also orders by rating in a descending way
    val filteredDf = dfApp
      .where(dfApp("Rating") > 4.0 && !isnan(dfApp("Rating")))
      .orderBy(desc("Rating"))

    filteredDf
  }

  def main(args: Array[String]): Unit = {

    // create spark session
    val ss = spark.sql.SparkSession
      .builder()
      .appName("Part2")
      .master("local") // single threaded
      .getOrCreate()

    try {
      val df_2 = run(ss)

      // get property values
      val rs = Funcs.getOrCreateProps()
      val fp = rs.getString("BEST_APPS_FP")

      // write results
      val tempFp = fp + "temp"
      df_2.write
        .option("header", "true")
        .option("delimiter", "§")
        .mode("overwrite")
        .csv(tempFp)

      Funcs.renameFile(fp, tempFp, ".csv")
    } finally {
      ss.close()
    }

  }

}
