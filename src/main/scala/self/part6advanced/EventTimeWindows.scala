package self.part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Window")
    .master("local[2]")
    .getOrCreate()


  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))


  def readPurchasesFromFile() = spark.readStream
    .format("json")
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    //      .schema(onlinePurchaseSchema)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchaseBySlidingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) /// time: struct column: has fields {start, end}
      .agg(sum("quantity").as("TotalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )
    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Same function as Sliding Window, Except the window() function doesn't have a third parameter (i.e. slideDuration)
   */
  def aggregatePurchaseByTumblingWindow(): Unit = {
    val purchasesDF = readPurchasesFromSocket()
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) /// time: struct column: has fields {start, end}
      .agg(sum("quantity").as("TotalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )
    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Exercises
   * 1) show the best selling product of every day.
   * 2) show the best selling product of every 24 hours, updated every hour
   */

    def bestSellingProductPerDay()={
      val purchasesDF = readPurchasesFromFile()
      val bestSellingWindowByDay = purchasesDF
//        .groupBy(window(col("time"), "1 day") // exercise 1 (Tumbling Window)
        .groupBy(window(col("time"), "24 hours", "1 hour") // exercise 2 (Sliding Window)
          .as("time"), col("item"))
        .agg(sum("quantity").as("totalQuantity"))
        .select(
          col("time").getField("start").as("start"),
          col("time").getField("end").as("end"),
          col("item"),
          col("totalQuantity")
        )
        .orderBy(col("time"), col("totalQuantity").desc)
      bestSellingWindowByDay.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
    }


  def main(args: Array[String]): Unit = {
//    aggregatePurchaseBySlidingWindow()
    bestSellingProductPerDay()



  }

}
