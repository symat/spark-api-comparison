package com.epam.meetup

import org.apache.spark.sql.SparkSession


object rdd_example {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Provide parameters in this order: moviesPath, actorPath")


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()


    spark.stop()
  }
}
