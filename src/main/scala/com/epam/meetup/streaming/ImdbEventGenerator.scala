package com.epam.meetup.streaming

import java.io.File
import java.util.regex.Pattern

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object DataType extends Enumeration {
  val ActorData, RatingData = Value
}


class ImdbEventGenerator(actorDataPath: String, ratingDataPath: String) {
  val YEAR_IN_FILE_NAME_PATTERN: Pattern = Pattern.compile("^.+_(\\d+)\\.tsv$")
  val yearsOfRatings = getYearsFromListOfFiles(ratingDataPath)
  val yearsOfActors = getYearsFromListOfFiles(actorDataPath)
  val years: List[String] = yearsOfRatings.intersect(yearsOfActors).toList.sorted


  def buildImdbEventStream(sc: SparkContext, dataType: DataType.Value): mutable.Queue[RDD[String]] = {
    val queue = mutable.Queue[RDD[String]]()
    val yearRDDs = years.map(year => sc.parallelize(loadLinesFromFile(year, dataType)))
    queue ++= yearRDDs
  }

  def getYearsFromListOfFiles(dir: String): Set[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles
        .filter(_.isFile)
        .map(_.getName)
        .map(YEAR_IN_FILE_NAME_PATTERN.matcher(_))
        .filter(_.matches())
        .map(_.group(1))
        .toSet
    } else {
      Set[String]()
    }
  }

  def loadLinesFromFile(year: String, dataType: DataType.Value): Seq[String] = {
    val fileName =  dataType match {
      case DataType.ActorData => s"$actorDataPath/actor_data_$year.tsv"
      case DataType.RatingData => s"$ratingDataPath/movie_ratings_$year.tsv"
    }
    val source = scala.io.Source.fromFile(fileName)
    try source.getLines().filter(!_.trim.isEmpty).toList finally source.close()
  }



}
