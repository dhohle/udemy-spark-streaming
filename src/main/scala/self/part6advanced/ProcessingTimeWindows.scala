package self.part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[*]")
    .getOrCreate()

  def aggregateByProcessingTime(): Unit = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(col("value"), current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"), "10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("sum")) // counting characters every 10 seconds by processing time
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("sum")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

  /*
  Window duration and sliding interval must be a multiple of the batch interval
  Output mode will influence results
   */
}
