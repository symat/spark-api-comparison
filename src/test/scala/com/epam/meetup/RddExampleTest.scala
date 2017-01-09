package com.epam.meetup

import org.junit.Test
import org.scalatest.FunSuite


class RddExampleTest  {


  val INPUT_ACTORS_SAMPLE = "data/actors_sample/"
  val INPUT_ACTRESSES_SAMPLE = "data/actresses_sample/"
  val INPUT_RATINGS_SAMPLE = "src/test/resources/test-input_ratings.json"


  @Test
  def shouldReturnCorrectScoresThroughSpark() = {

    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_RATINGS_SAMPLE))


  }


}
