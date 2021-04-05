package self.part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp
import scala.concurrent.duration._

object Watermarks {
  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = // useful skill for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]" else query.lastProgress.eventTime //(Map[String, String])
        println(s"$i: $queryEventTime")
      }
    }).start()

  // 3000, blue
  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map({ line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        (timestamp, data)
      })
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds") // adding a 2 sec watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")


    /*
    A 2 second watermark means
    - a window will only be considered until the watermark surpasses the window end
    - an element/a row/a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      // setting the batch time
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)

    query.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    testWatermark()
  }

  /*
  Add watermarks to a time column
  - val enhancedDF = df.withWatermark("time", waterMarkDuration)
  With every batch, Spark will
  - update the max time ever recorded
  - update watermark as (max time - watermark duration)
  Guarantees
  - in every batch, all records with time > watermark will be considered
  - if using window functions, a window will be updated until the watermark surpasses the window
  No guarantees
  - records whose time < watermark will not necessarily be dropped
  Aggregations & joins in append mode need watermarks
  - a watermark allows Spark to drop old records from state management
   */
}


object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() //blocking call (waits until something is listening)
  val printer = new PrintStream(socket.getOutputStream)

  println("socket.accepted")

  def example1(): Unit = {
    Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000, green")
    Thread.sleep(4000)
    printer.println("14000, blue")
    Thread.sleep(1000)
    printer.println("9000, red") // discarded: older than the watermark
    Thread.sleep(3000)
    printer.println("15000, red")
    printer.println("8000, blue") // discarded: older than the watermark
    Thread.sleep(1000)
    printer.println("13000, green")
    Thread.sleep(500)
    printer.println("8000, green")
    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped
    Thread.sleep(2000)
    printer.println("17000,green")
  }

  def example2(): Unit = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")
    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }


  def example3(): Unit = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red")
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green")
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }

  def main(args: Array[String]): Unit = {
    //    example1()
//    example2()
    example3()
  }


}