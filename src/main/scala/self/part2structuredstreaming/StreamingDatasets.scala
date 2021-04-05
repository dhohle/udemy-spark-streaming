package self.part2structuredstreaming

import common.{Car, carsSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations

  import spark.implicits._

  def readCars(): Dataset[Car] = {
    //    val carEncoder = Encoders.product[Car] // needed if spark.implicits._ is not loaded

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column value
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] //(carEncoder) //optionally, add the encoder explicitly


  }

  def showCarNames(): Unit = {
    val carsDS: Dataset[Car] = readCars()

    // transformations
    val carNames: DataFrame = carsDS.select(col("name")) // DF

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name).as("CarName")

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /*
  1) count how many POWERFUL cars we have in the DS (HP > 140)
  2) Average HP for the entire dataset
    (use the complete output mode)
  3) Count the cars by origin
   */

  def countPowerfulCars(): Unit = {
    val carsDS = readCars();

    val countedDS = carsDS.filter(_.Horsepower.getOrElse(0l) > 140)

    //    println(countedDS)
    countedDS.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def averageHorsepowerCars(): Unit = {
    val carsDS = readCars()
    carsDS.select(avg(col("Horsepower")).as("avg_horsepower"))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def countCarsByOrigin(): Unit ={
    val carsDS = readCars()
//    carsDS.groupBy("Origin").count()
    carsDS.groupByKey(_.Origin).count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
//        showCarNames()
//    countPowerfulCars()
//        averageHorsepowerCars()
    countCarsByOrigin()
  }
}
