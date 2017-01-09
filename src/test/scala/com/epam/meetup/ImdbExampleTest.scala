package com.epam.meetup

import org.junit.Test


class ImdbExampleTest {

  val INPUT_ACTORS_SAMPLE = "data/actors_sample/"
  val INPUT_ACTRESSES_SAMPLE = "data/actresses_sample/"
  val INPUT_RATINGS_SAMPLE = "data/ratings.tsv"
  val OUTPUT_PATH = "data/output"


  @Test
  def shouldCalculateUsingRdd() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_SAMPLE, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingDataFrames() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    DataFrameExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_SAMPLE, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingSQL() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    SQLExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_SAMPLE, OUTPUT_PATH))
  }


}
