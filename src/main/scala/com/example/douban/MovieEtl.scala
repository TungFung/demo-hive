package com.example.douban

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * spark-submit --class com.twq.douban.MovieEtl \
*--master spark://master-dev:7077 \
*--deploy-mode client \
*--driver-memory 512m \
*--executor-memory 512m \
*--total-executor-cores 2 \
*--executor-cores 1 \
*--conf spark.douban.movie.path=hdfs://master:9999/user/hadoop-twq/hive-course/douban/movie.csv \
* home/hadoop-twq/hive-course/hive-course-1.0-SNAPSHOT.jar douban movie
*/
object MovieEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieEtl")
      .master("local[4]")
      .config("hive.metastore.uris", "thrift://master:9083")
      .config("hive.metastore.warehouse.dir", "hdfs://master:9999/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext
      .textFile("hdfs://master:9999/user/hadoop-twq/hive-course/movie.csv")
      .map(line => MovieDataParser.parseLine(line)).toDS()
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year")
      .saveAsTable(s"douban.movie")

    val df: Dataset[Row] = spark.read.table("douban.movie")
    df.show()

    spark.stop()
  }
}
