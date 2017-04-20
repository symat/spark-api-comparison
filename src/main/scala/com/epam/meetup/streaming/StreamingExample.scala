package com.epam.meetup.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream


object StreamingExample {


  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Provide parameters in this order: actorsDataFolderPath, ratingEventsDataFolderPath, outputPath")

    val actorsFolder = args(0);
    val ratingFolder = args(1);
    val outputPath = args(2);

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Imdb - Spark Core")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(2))
    val imdbEventGenerator = new ImdbEventGenerator(actorsFolder, ratingFolder)

    val actorsOfMovies: InputDStream[String] = sparkStreamingContext.queueStream(imdbEventGenerator.buildImdbEventStream(sparkContext, DataType.ActorData))
    val movieRatingEvents: InputDStream[String] = sparkStreamingContext.queueStream(imdbEventGenerator.buildImdbEventStream(sparkContext, DataType.RatingData))

    // actorsOfMovies.print(15)
    // movieRatingEvents.print(15)


    // ============================================
    // =====       ADD YOUR CODE HERE!!      ======
    // ============================================


    sparkStreamingContext.start()             // Start the computation
    sparkStreamingContext.awaitTermination()  // Wait for the computation to terminate
    spark.stop()
  }
}
