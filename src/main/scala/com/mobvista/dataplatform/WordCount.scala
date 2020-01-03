package com.mobvista.dataplatform

import org.apache.spark.sql.SparkSession
object WordCount {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .config("spark.rdd.compress", "true")
     .config("spark.speculation", "false")
     .config("spark.io.compression.codec", "snappy")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .getOrCreate()
    val sc = spark.sparkContext
    sc.textFile("oss://mob-emr-test/lei.du/word.txt",2)
      .flatMap(line=> line.split(" "))
      .map(word=>(word,1))
      .reduceByKey((a,b)=> a+b)
      .saveAsTextFile("oss://mob-emr-test/lei.du/word_count_result")
  }
}
