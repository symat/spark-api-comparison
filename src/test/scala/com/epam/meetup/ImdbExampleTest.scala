package com.epam.meetup

import org.junit.Test


class ImdbExampleTest {

  val INPUT_ACTORS = "data/actors/"
  val INPUT_ACTORS_SAMPLE = "data/actors_sample/"
  val INPUT_ACTRESSES = "data/actresses/"
  val INPUT_ACTRESSES_SAMPLE = "data/actresses_sample/"
  val INPUT_RATINGS_TOP_250 = "data/ratings_top250_movies.tsv"
  val OUTPUT_PATH = "data/output"


  @Test
  def shouldCalculateUsingRdd() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    RddExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingDataFrames() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    DataFrameExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingSQL() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    SQLExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingDataSet() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    DataSetExample.main(Array(INPUT_ACTORS_SAMPLE, INPUT_ACTRESSES_SAMPLE, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }

  @Test
  def shouldCalculateUsingMoreCleverSql() = {
    TestUtils.deleteFolder(OUTPUT_PATH)
    SQLExampleMoreClever.main(Array(INPUT_ACTORS, INPUT_ACTRESSES, INPUT_RATINGS_TOP_250, OUTPUT_PATH))
  }


}
