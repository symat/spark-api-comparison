package com.epam.meetup

import org.apache.spark.sql.SparkSession


object DataSetExample {

  case class ActorMovieRelation(name: String, movieTitle: String, movieYear: String)

  case class MovieRating(distributionOfVotes: String, numberOfVotes: Int, rating: Float, movieTitle: String, movieYear: String)

  case class ActorRating(name: String, rating: Float)

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: actorsTsvFolderPath, actressesTsvFolderPath, ratingTsvFilePath, outputPath")

    val actorsFolder = args(0);
    val actressesFolder = args(1);
    val ratingFile = args(2);
    val outputPath = args(3);

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Imdb - Spark DataFrame")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    // this is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val maleActorMovieRelations = loadTsvFile(actorsFolder, spark)
      .map(row => new ActorMovieRelation(row.getString(0), row.getString(1), row.getString(2)))

    val femaleActorMovieRelations = loadTsvFile(actorsFolder, spark)
      .map(row => new ActorMovieRelation(row.getString(0), row.getString(1), row.getString(2)))

    val allActorMovieRelations = maleActorMovieRelations
      .union(femaleActorMovieRelations)

    allActorMovieRelations.show(10)
    allActorMovieRelations.printSchema()

    println(s"number of movie-actor pairs: ${allActorMovieRelations.count()}")

    val movieRatings = loadTsvFile(ratingFile, spark)
      .map(row => new MovieRating(row.getString(0), row.getString(1).toInt, row.getString(2).toFloat, row.getString(3), row.getString(4)))

    println(s"number of movie-rating pairs: ${movieRatings.count()}")

    val allPeopleWithRatedMovies = allActorMovieRelations
      .joinWith(movieRatings,
        allActorMovieRelations("movieTitle") === movieRatings("movieTitle") &&
          allActorMovieRelations("movieYear") === movieRatings("movieYear"))
      .map(a => new ActorRating(a._1.name, a._2.rating))

    val allPeopleWithAverageRates = allPeopleWithRatedMovies
      .groupBy("name")
      .avg("rating")

    allPeopleWithAverageRates.show(10)

    allPeopleWithAverageRates.rdd.saveAsTextFile(outputPath)

    spark.stop()
  }

  def loadTsvFile(path: String, sparkSession: SparkSession) = {
    sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .load(path)
  }
}
