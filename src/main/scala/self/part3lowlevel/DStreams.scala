package self.part3lowlevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

object DStreams {
  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()


  /**
   * Spark Streaming Context = entry point to the DStreams API
   * - needs the spark interval
   * - a duration = batch interval
   * Polls each second
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
  - define input sources by creating DStreams
  - define transformations on DStreams
  - call an action on DStreams
  - start ALL computations with ssc.start()
    - no more computations can be added
  - await termination, or stop the computation
    - you cannot restart a computation
   */

  def readFromSocket(): Unit = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(_.split(" "))

    // action
//    wordsStream.print()

    wordsStream.saveAsTextFiles("src/main/resources/data/words") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
    //  ssc.awaitTerminationOrTimeout(10000L)
  }

  def createNewFile(): Unit = {
    new Thread(() => {
      Thread.sleep(5000)
      val path = "src/main/resources/data/stocks"
      val dir = new File(path) //
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()
      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
          |AAPL,Mar 1 2001,11.03
          |AAPL,Apr 1 2001,12.74
          |AAPL,May 1 2001,9.98
          |""".stripMargin.trim)
      writer.close()
    }).start()
  }

  def readFromFile(): Unit = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"
    /**
     * ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream = ssc.textFileStream(stocksFilePath)

    // transformation
    val dateFormat = new SimpleDateFormat("MMM d yyyy", Locale.ENGLISH)
    val stockStream: DStream[Stock] = textStream.map({ line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val value = tokens(2).toDouble
      Stock(company, date, value)
    })

    // call an action
    stockStream.print()
    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {

//    readFromSocket()

//    readFromFile()
  }

}
