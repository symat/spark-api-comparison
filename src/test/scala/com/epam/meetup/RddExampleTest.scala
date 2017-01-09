package com.epam.meetup

import org.junit.Test


class RddExampleTest {


  val INPUT_ACTORS_SAMPLE = "data/actors_sample/"
  val INPUT_ACTRESSES_SAMPLE = "data/actresses_sample/"
  val INPUT_RATINGS_SAMPLE = "data/ratings.tsv"
  val OUTPUT_PATH = "data/output"


  @Test
  def shouldReturnCorrectScoresThroughSpark() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_SAMPLE, OUTPUT_PATH))
  }


}
