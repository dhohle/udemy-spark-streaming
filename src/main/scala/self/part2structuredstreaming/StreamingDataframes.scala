package self.part2structuredstreaming

import common._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import self.Statics

import scala.concurrent.duration.DurationInt

object StreamingDataframes {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Our first streams")
    .config("spark.master", "local[2]") // 2 threads
    .getOrCreate()

  def readFromSocket() = {
    // Reading a DataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformations
    val shortLines = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // Wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles(): Unit = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load(Statics.dirPrefix("stocks"))


    stocksDF.writeStream
      .format("console")
      .outputMode("append") // append is default
      .start()
      .awaitTermination()
  }

  def demoTriggers(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
//        Trigger.Once() // Single Batch then terminate
        Trigger.Continuous(2.seconds) // Every 2 seconds -> create a batch with whatever you have (also if it's nothing)
//        Trigger.ProcessingTime(2.seconds)//every 2 seconds -> run the query
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    //    readFromSocket()
    //    readFromFiles()
    demoTriggers()
  }

}
