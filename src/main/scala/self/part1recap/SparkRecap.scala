package self.part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, max}
import self.Statics

object SparkRecap  {

  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[*]")
    .getOrCreate()

  val cars = spark.read.format("json")
    .option("inferSchema", "true")
    .load(Statics.dirPrefix("cars"))


  def main(args: Array[String]): Unit = {
    cars.show()
    cars.printSchema()
    import spark.implicits._


    // select
    val usefulCarsData = cars.select(
      col("Name"), // column name
      $"Year", // Another column object (Needs spark implicits)
      (col("Weight_in_lbs")/2.2).as("Weight_in_kg"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2"),
    )

    val carsWeight = cars.selectExpr("Weight_in_lbs / 2.2")

    // filter
    val europeanCars = cars.where(col("Origin") =!= "USA")

    // aggregations
    val averageHP = cars.select(avg(col("Horsepower")).as("average_hp"))

    // grouping
    val groupByOrigin = cars.groupBy("Origin")
//      .count()
    groupByOrigin.count().show()
    groupByOrigin.agg(
      avg("Weight_in_lbs").as("avg_weight"),
      max("Weight_in_lbs").as("max_weight")
    ).show()


    // joining
    val guitarPlayersDF = spark.read.option("inferSchema","true").json(Statics.dirPrefix("guitarPlayers"))
    val bandsDF = spark.read.option("inferSchema","true").json(Statics.dirPrefix("bands"))

    val guitaristsBands = guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"))
    guitaristsBands.show()

    case class GuitarPlayer(id: Int, name: String, guitars: Seq[Long], band: Long)

//    val guitarPlayersDS = guitarPlayersDF.as[GuitarPlayer]

    // Spark SQL
    cars.createOrReplaceTempView("cars")
    val americanCars = spark.sql(
      """
        |select Name from cars where Origin = 'USA'
        |""".stripMargin
    ).show()

    // low-level API: RDDs
    val sc = spark.sparkContext
    val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

    //functional operators
    val doubles = numbersRDD.map(_*2)

    // RDD -> DF
    val numbersDF = numbersRDD.toDF("number") // you lose type info, you get SQL capability

    // RDD -> DS
    val numbersDS = spark.createDataset(numbersRDD) //

    // DS -> RDD
//    val guitarPlayersRDD = guitarPlayersDS.rdd

    // DF -> RDD
    val guitarPlayersRDD2 = guitarPlayersDF.rdd


  }
}
