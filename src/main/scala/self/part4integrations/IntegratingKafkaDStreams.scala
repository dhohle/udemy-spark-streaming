package self.part4integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    //    "bootstrap.servers"-> "localhost:9092,anotherserver:port",
    // kafka serializer
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object] // false is not an instance of Object
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka(): Unit = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      /*
      distribute the partitions evenly across the Spark Cluster
      Alternatives:
      - PreferBrokers: if the brokers and executors are in the same cluster
      - PreferFixed
       */
      LocationStrategies.PreferConsistent,

      /**
       * Alternative
       * - SubscribePattern; allow subscribing to topics matching a pattern
       * - Assign - advanced: allows specifying offsets and partitions per topic
       */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1")) // every stream needs a group.id
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka(): Unit = {
    val inputData = ssc.socketTextStream("localhost", 12345)
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD({ rdd =>
      rdd.foreachPartition({ partition =>
        // inside the lambda, the code is run by a single executor
        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach({ pair =>
          kafkaHashMap.put(pair._1, pair._2)
        })
        // producer can insert records into the Kafka topics
        // available on this executor
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach({ value =>
          val key = null
          val message = new ProducerRecord[String, String](kafkaTopic, key, value)
          // feed message into the Kafka topic
          producer.send(message)
        })

        producer.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
//    readFromKafka()
    writeToKafka()
  }
}
