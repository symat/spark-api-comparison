package com.epam.meetup

import com.epam.meetup.streaming.StreamingExample
import org.junit.Test


class ImdbStreamingExampleTest {

  val INPUT_ACTORS_TOP_250_IMDB_MOVIES = "data/streaming_data/actor_data"
  val INPUT_RATING_EVENTS_TOP_250_IMDB_MOVIES = "data/streaming_data/movie_ratings"

  val INPUT_ACTORS_TEST = "data/streaming_data_test/actor_data"
  val INPUT_RATING_EVENTS_TEST = "data/streaming_data_test/movie_ratings"

  @Test
  def shouldCalculateTopActors() = {
    val minimumNumberOfRates = 5000000
    StreamingExample.main(Array(INPUT_ACTORS_TOP_250_IMDB_MOVIES, INPUT_RATING_EVENTS_TOP_250_IMDB_MOVIES, minimumNumberOfRates.toString))

    // ===== send me the top 10 actors from the output, after whole stream processed!   =====

  }

  @Test
  def shouldCalculateTopActorsOnSmallTestDataSet() = {
    val minimumNumberOfRates = 0
    StreamingExample.main(Array(INPUT_ACTORS_TEST, INPUT_RATING_EVENTS_TEST, minimumNumberOfRates.toString))

    // ============= expected output after whole stream processed:  ==================
    //  actor: Mr Rick, average rate:  6.0, number of rates: 1000
    //  actor: Ms Anne, average rate:  5.4, number of rates: 5000
    //  actor: Mr John, average rate:  4.7777777, number of rates: 9000
    //

  }


  @Test
  def shouldCalculateTopActorsOnSmallTestDataSetWithFiltering() = {
    val minimumNumberOfRates = 6000
    StreamingExample.main(Array(INPUT_ACTORS_TEST, INPUT_RATING_EVENTS_TEST, minimumNumberOfRates.toString))

    // ============= expected output after whole stream processed:  ==================
    //  actor: Mr John, average rate:  4.7777777, number of rates: 9000
    //

  }


}
