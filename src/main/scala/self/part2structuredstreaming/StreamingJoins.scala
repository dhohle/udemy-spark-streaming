package self.part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import self.Statics

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers = spark.read.option("inferSchema", "true").json(Statics.dirPrefix("GuitarPlayers"))
  val guitars = spark.read.option("inferSchema", "true").json(Statics.dirPrefix("guitars"))
  val bands = spark.read.option("inferSchema", "true").json(Statics.dirPrefix("bands"))

  // joining static DFs
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  val bandsSchema = bands.schema

  def joinStreamWithStatic(): Unit = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val streamedBandsGuitarists = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")


    /**
     * restricted joins:
     * - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
     * - static joining with streaming: LEFT outer joint/full outer join/left_semi not permitted
     */

    streamedBandsGuitarists.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  // since Spark 2.3 we have stream with stream joins
  def joinStreamWithStream(): Unit ={
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load() // DF with a single column "value" of type String
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"), "inner")

    /*
    - inner joins are supported
    - left/right outer joins ARE supported, but must have watermarks
     */
    streamedJoin.writeStream
      .format("console")
      .outputMode("append")// only append supported for stream vs stream join
      .start()
      .awaitTermination()


  }

  def main(args: Array[String]): Unit = {
//    joinStreamWithStatic()
    joinStreamWithStream()

  }

}
