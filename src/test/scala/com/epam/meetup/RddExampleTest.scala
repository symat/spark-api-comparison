package com.epam.meetup

import org.junit.Test
import org.scalatest.FunSuite


class RddExampleTest  {


  val INPUT_ACTORS_SAMPLE = "src/test/resources/test-input_actors.json"
  val INPUT_RATINGS_SAMPLE = "src/test/resources/test-input_ratings.json"


  @Test
  def shouldReturnCorrectScoresThroughSpark() = {

    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_RATINGS_SAMPLE))


  }


}
