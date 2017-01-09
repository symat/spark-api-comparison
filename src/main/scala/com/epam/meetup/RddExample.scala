package com.epam.meetup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object RddExample {


  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: actorsTsvFolderPath, actressesTsvFolderPath, ratingTsvFilePath, outputPath")

    val actorsFolder = args(0);
    val actressesFolder = args(1);
    val ratingFile = args(2);
    val outputPath = args(3);

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Imdb - Spark Core")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val maleActorFile: RDD[String] = sparkContext.textFile(actorsFolder, minPartitions = 2)
    val femaleActorFile: RDD[String] = sparkContext.textFile(actressesFolder, minPartitions = 2)

    val allMovieActorPairs: RDD[(String, String)] = sparkContext.union(maleActorFile, femaleActorFile)
      .map(line => line.split("\t"))
      .filter(columns => columns.length == 3)
      .filter(columns => columns(2).matches("[\\d]{4}")) // filter invalid years, like "????"
      .map(columns => (s"${columns(1)}-${columns(2)}", columns(0))) // title-year, actress

    println(s"number of movie-actor pairs: ${allMovieActorPairs.count()}")

    val ratings: RDD[(String, Float)] = sparkContext.textFile(ratingFile)
      .map(line => line.split("\t"))
      .filter(columns => columns.length == 5)
      .filter(columns => columns(4).matches("[\\d]{4}")) // filter invalid years, like "????"
      .map(columns => (s"${columns(3)}-${columns(4)}", columns(2).toFloat)) // title-year, rating

    println(s"number of movie-rating pairs: ${ratings.count()}")

    val allPeopleWithAverageRatings: RDD[(String, Float)] = allMovieActorPairs
      .join(ratings) // ( movieTitle-year, (actorName, movieRating) )
      .map(_._2) // (actorName, movieRating)
      .aggregateByKey((0f, 0f))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(aggregatedSumAndCount => 1f * aggregatedSumAndCount._1 / aggregatedSumAndCount._2)

    println(s"number of actor-rating pairs: ${allPeopleWithAverageRatings.count()}\n")
    println("sample: ")
    allPeopleWithAverageRatings.take(10).foreach(a => println(s"${a._1}: ${a._2}"))
    println("...")

    allPeopleWithAverageRatings.saveAsTextFile(outputPath)

    spark.stop()
  }
}
