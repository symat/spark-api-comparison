package com.epam.meetup

import com.epam.meetup.batch._
import com.epam.meetup.streaming.StreamingExample
import org.junit.{Before, Test}


class ImdbStreamingExampleTest {

  val INPUT_ACTORS = "data/streaming_data/actor_data"
  val INPUT_RATING_EVENTS_TOP_250 = "data/streaming_data/movie_ratings"
  val OUTPUT_PATH = "data/output"


  @Test
  def shouldCalculateUsingRdd() = {
    StreamingExample.main(Array(INPUT_ACTORS, INPUT_RATING_EVENTS_TOP_250, OUTPUT_PATH))
  }

  @Before
  def setup() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
  }

}
