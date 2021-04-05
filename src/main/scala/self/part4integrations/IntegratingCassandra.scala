package self.part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import part4integrations.IntegratingCassandra.CarCassandraForeachWriter

object IntegratingCassandra {


  val spark = SparkSession.builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  import spark.implicits._
  def writeStreamToCassandraInBatches(): Unit ={
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      // static dataset
      .foreachBatch({ (batch: Dataset[Car], batchId:Long) =>
        // save this batch to Cassandra in a single table write
        batch.select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public") // type enrichment
          .mode(SaveMode.Append)
          .save()
      })
      .start()
      .awaitTermination()
  }

  class CarCassandraForeachWriter extends ForeachWriter[Car]{
    /*
    - on every batch, on every partition `partitionId`
      - on every "epoch" = chunk of data
        - call the open method: if false, skip this chunk
        - for each entry in this chunk ,  call the process method
        - call the close method, either at the end of the chunnk or with an error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)
    override def open(partitionId: Long, epochId: Long): Boolean = {
      print("Open Connections")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo({session=>
        session.execute(
          s"""
             |insert into $keyspace.$table("Name", "Horsepower")
             |values('${car.Name}', ${car.Horsepower.orNull})
             |""".stripMargin)
      })
    }

    override def close(errorOrNull: Throwable): Unit = {
      println("Closing connection")
    }
  }
  def writeStreamToCassandra(): Unit ={
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      // static dataset
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    writeStreamToCassandraInBatches()
    writeStreamToCassandra()
  }

}
