package challenge

import org.apache.spark.sql.functions.{asc, coalesce, collect_set, desc, lit, split, udf}
import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Part3 {

  /**
   * Process the size, converting the values to M
   */
  private val processSize = udf((size: String) => ps(size))
  private def ps(size: String): Double = {
    // in case string is empty
    if (size == null || size.trim.length < 1) {
      return -1.0
    }

    // extract unit char and the value
    var s = 0.0
    val unit = size.takeRight(1)
    val value = size.dropRight(1).toDoubleOption

    // in case value is not a double
    if (value.isEmpty) {
      return -1.0
    }

    // get the value and convert it, depending on the unit
    unit.toLowerCase() match {
        case "m" => s = value.get
        case "k" => s = value.get / 1000.0
    }

    s
  }

  /**
   * Process the price, giving it a string and converting to a double
   */
  private val processPrice = udf((price: String) => pp(price))
  private def pp(price: String): Double = {

    // verify if string is empty
    if (price == null || price.trim.length < 2) {
      return -1.0
    }

    var p = 0.0
    val currency = ""+price.charAt(0) // get first char, which should be the currency
    val value = price.substring(1).toDoubleOption // gets the remaining string, which should be the value

    // in case the value extracted cannot be converted to a double
    if (value.isEmpty) {
      return -1.0
    }

    // conversions to €
    currency match {
      case "$" => p = value.get * 0.9
    }

    p
  }

  def run(ss: SparkSession): DataFrame = {
    val props = Funcs.getOrCreateProps()

    val dfApp = ss.read
      .option("delimiter", ",")
      .option("header", "true")
      .csv(props.getString("APP_FP"))

    // conversions
    var newDf = dfApp.withColumn("Reviews", dfApp("Reviews").cast(LongType))
    newDf = newDf.withColumn("Rating", newDf("Rating").cast(DoubleType))
    newDf = newDf.withColumn("Last Updated", newDf("Last Updated").cast(DateType))

    // renaming
    newDf = newDf.withColumnsRenamed(Map(
      "Content Rating" -> "Content_Rating",
      "Last Updated" -> "Last_Updated",
      "Current Ver" -> "Current_Version",
      "Android Ver" -> "Minimum_Android_Version"
    ))

    // processing

    // process categories: aggregate Category first, creating a new column "Categories"
    val aggDf = newDf.groupBy("App").agg(collect_set("Category").as("Categories"))
    // join the newly created DF with the old one, so it can add the remaining column
    newDf = aggDf.join(newDf, Seq("App"))
    // Order it by App and Reviews (desc)
    newDf = newDf.orderBy(asc("App"), desc("Reviews"))
    // drop any rows with the same "App" name
    newDf = newDf.dropDuplicates("App").drop("Category")

    // process size
    newDf = newDf.withColumn("Size", processSize(newDf("Size")))
    // process price
    newDf = newDf.withColumn("Price", processPrice(newDf("Price")))
    // process genres
    newDf = newDf.withColumn("Genres", split(newDf("Genres"), ";").cast(ArrayType(StringType)))

    // default values
    newDf = newDf.withColumn("Reviews", coalesce(newDf("Reviews"), lit(0)))

    newDf
  }

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("Part3")
      .master("local")
      .getOrCreate()

    try {
      val df_3 = run(ss)
    } finally {
      ss.close()
    }

  }

}
