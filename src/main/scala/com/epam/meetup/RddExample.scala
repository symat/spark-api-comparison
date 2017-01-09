package com.epam.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

import spray.json._
import DefaultJsonProtocol._

case class MovieActors(title: String, actors: List[String])


object RddExample {


  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Provide parameters in this order: actorsTsvFolderPath, actressesTsvFolderPath, ratingTsvFilePath")

    val actorsFolder = args(0);
    val actressesFolder = args(1);
    val ratingFile = args(2);

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Imdb - Spark Core")
      .getOrCreate()



    val actorsRdd: RDD[MovieActors] = spark.sparkContext.textFile(actorsFile)
      .map(line => line.parseJson)
      .map(json => {
        implicit lazy val modelFormat = jsonFormat2(MovieActors)
        json.convertTo[MovieActors]
      })

    actorsRdd.foreach(a => System.out.println(a))

    spark.stop()
  }
}
