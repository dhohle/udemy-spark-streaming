package self.part4integrations

import common.carsSchema
import org.apache.spark.sql.functions.{col, expr, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[*]")
    .getOrCreate()


  def readFromKafka(): Unit = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      //      .option("subscribe", "rockthejvm, anothertopic")
      .load()


    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  /**
   * If you want to rerun this method, make sure the checkpoint directory is deleted;
   * otherwise, Kafka will know this data has already been written
   */
  def writeToKafka(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    // need key and value
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints, the writing to kafka will fail
      .start()
      .awaitTermination()
  }

  /**
   * Excercise
   */
  def writeCarsJsonToKafka(): Unit = {
    val carsDF = spark.readStream.schema(carsSchema).json("src/main/resources/data/cars")
    //
    val carsKafkaDF: DataFrame = carsDF
      .select(col("Name").as("key"),
        to_json(struct("*")).cast("String").as("value") // all
        // to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value") // selection
      )

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints, the writing to kafka will fail
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //    readFromKafka()
    //    writeToKafka()
    writeCarsJsonToKafka()

  }
}
