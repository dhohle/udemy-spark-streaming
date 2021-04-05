package self.part3lowlevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformations {

  val spark = SparkSession.builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  // entry
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val path = "src\\main\\resources\\data\\people-1m"

  def readPeople() = ssc.socketTextStream("localhost", 9999).map({ line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt, //id
      tokens(1), // first name
      tokens(2), // middle name
      tokens(3), /// last name
      tokens(4), // gender
      Date.valueOf(tokens(5)), //birth
      tokens(6), // ssn/uuid
      tokens(7).toInt // salary
    )
  })

  // map, flatMap, filter
  def peopleAges(): DStream[(String, Int)] = readPeople().map({ person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  })

  def peopleSmallNames() = readPeople().flatMap({person =>
    List(person.firstName, person.middleName)
  })

  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  def countPeople() = readPeople().count()// the number of entries in every batch

  // Count by value, PER BATCH
  def countNames() = readPeople().map(_.firstName).countByValue()

  import spark.implicits._ // for encoders to create dataset

  /*
   reduce by key
   - works on DStream of tuples
   - works per batch
   */
  def countNamesReduce() =
    readPeople()
      .map(_.firstName)
      .map(name => (name, 1))
      .reduceByKey((a, b) => a + b)

  /*
  for each RDD
   */
  def saveToJson() = readPeople().foreachRDD({ rdd =>
    val ds = spark.createDataset(rdd)//[Person]
    val f = new File("src/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"
    ds.write.json(path)

  })

  def main(args: Array[String]): Unit = {
//    val stream = readPeople()
//    val stream = peopleAges()
//    val stream = peopleSmallNames()
//    val stream = highIncomePeople()
//    val stream = countPeople()
//    val stream = countNames()
//    val stream = countNamesReduce()

//    stream.print()

    saveToJson()
    ssc.start()
    ssc.awaitTermination()


  }
}
