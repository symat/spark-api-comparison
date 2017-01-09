package com.epam.meetup

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType


object DataFrameExample {


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
    val femaleActorMovieRelations = loadTsvFile(actorsFolder, spark)

    //maleActorMovieRelations.show(10)
    //maleActorMovieRelations.printSchema()

    val allActorMovieRelations = maleActorMovieRelations
      .union(femaleActorMovieRelations)
      .withColumnRenamed("_c0", "name")
      .withColumnRenamed("_c1", "movieTitle")
      .withColumnRenamed("_c2", "movieYear")
      .where($"movieYear" =!= "????")

    //allActorMovieRelations.show(10)
    //allActorMovieRelations.printSchema()

    println(s"number of movie-actor pairs: ${allActorMovieRelations.count()}")

    val movieRatings = loadTsvFile(ratingFile, spark)
      .withColumn("rating", $"_c2".cast(FloatType))
      .withColumnRenamed("_c3", "movieTitle")
      .withColumnRenamed("_c4", "movieYear")
      .drop("_c0")
      .drop("_c1")
      .drop("_c2")

    println(s"number of movie-rating pairs: ${movieRatings.count()}")

    val allPeopleWithRatedMovies = allActorMovieRelations
      .join(movieRatings,
            allActorMovieRelations("movieTitle") === movieRatings("movieTitle") &&
            allActorMovieRelations("movieYear") === movieRatings("movieYear"))

    //allPeopleWithRatedMovies.show(10)

    val allPeopleWithAverageRates = allPeopleWithRatedMovies
      .groupBy("name")
      .avg("rating")

    allPeopleWithAverageRates.show(10)

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
