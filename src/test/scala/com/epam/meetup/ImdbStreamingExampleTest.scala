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
    val minimumNumberOfVotes = 5000000
    val minimumNumberOfMovies = 5

    StreamingExample.main(Array(
      INPUT_ACTORS_TOP_250_IMDB_MOVIES,
      INPUT_RATING_EVENTS_TOP_250_IMDB_MOVIES,
      minimumNumberOfVotes.toString,
      minimumNumberOfMovies.toString
    ))

    // ===== send me the results, after the whole stream processed!   =====

  }

  @Test
  def shouldCalculateTopActorsOnSmallTestDataSet() = {
    val minimumNumberOfVotes = 0
    val minimumNumberOfMovies = 0

    StreamingExample.main(Array(
      INPUT_ACTORS_TEST,
      INPUT_RATING_EVENTS_TEST,
      minimumNumberOfVotes.toString,
      minimumNumberOfMovies.toString
    ))

    // ============= expected output after whole stream processed:  ==================
    //
    //  ==== TASK 1: busy actors ====
    //  actor: Mr John, number of movies:  5
    //  actor: Ms Anne, number of movies:  2
    //  actor: Mr Rick, number of movies:  1
    //
    //  ==== TASK 2: best actors (at least 0 votes) ====
    //  actor: Mr Rick, average rate:  6.0, number of votes: 1000
    //  actor: Ms Anne, average rate:  5.4, number of votes: 5000
    //  actor: Mr John, average rate:  4.7777777, number of votes: 9000
    //
    //  ==== TASK 3: best busy actors (at least 0 movies) ====
    //  actor: Mr Rick, average rate:  6.0, number of movies: 1
    //  actor: Ms Anne, average rate:  5.4, number of movies: 2
    //  actor: Mr John, average rate:  4.7777777, number of movies: 5


  }


  @Test
  def shouldCalculateTopActorsOnSmallTestDataSetWithFiltering() = {
    val minimumNumberOfVotes = 6000
    val minimumNumberOfMovies = 2

    StreamingExample.main(Array(
      INPUT_ACTORS_TEST,
      INPUT_RATING_EVENTS_TEST,
      minimumNumberOfVotes.toString,
      minimumNumberOfMovies.toString
    ))

    // ============= expected output after whole stream processed:  ==================
    //
    //  ==== TASK 1: busy actors ====
    //  actor: Mr John, number of movies:  5
    //  actor: Ms Anne, number of movies:  2
    //  actor: Mr Rick, number of movies:  1
    //
    //  ==== TASK 2: best actors (at least 6000 votes) ====
    //  actor: Mr John, average rate:  4.7777777, number of votes: 9000
    //
    //  ==== TASK 3: best busy actors (at least 2 movies) ====
    //  actor: Ms Anne, average rate:  5.4, number of movies: 2
    //  actor: Mr John, average rate:  4.7777777, number of movies: 5


  }


}
