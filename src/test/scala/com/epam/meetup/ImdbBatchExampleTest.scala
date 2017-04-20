package com.epam.meetup

import com.epam.meetup.batch._
import org.junit.{Before, Test}


class ImdbBatchExampleTest {

  val INPUT_ACTORS = "data/actors/"
  val INPUT_ACTORS_SAMPLE = "data/actors_sample/"
  val INPUT_ACTRESSES = "data/actresses/"
  val INPUT_ACTRESSES_SAMPLE = "data/actresses_sample/"
  val INPUT_RATINGS_TOP_250 = "data/ratings_top250_movies.tsv"
  val OUTPUT_PATH = "data/output"


  @Test
  def shouldCalculateUsingRdd() = {
    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingDataFrames() = {
    DataFrameExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingSQL() = {
    SQLExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingDataSet() = {
    DataSetExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingMoreCleverSql() = {
    SQLExampleMoreClever.main(Array(INPUT_ACTORS, INPUT_ACTRESSES, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Before
  def setup() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
  }

}
