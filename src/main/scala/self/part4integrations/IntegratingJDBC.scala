package self.part4integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]
  //  will only work if the table does not exist
    // if it complains about incorrect password; close already running postgres database (local)
    carsDS.writeStream
      .foreachBatch({ (batch: Dataset[Car], batchId: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      })
      .start
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()

  }

}
