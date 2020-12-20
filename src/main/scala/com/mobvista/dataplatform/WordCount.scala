package com.mobvista.dataplatform
import java.net.URI
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
object WordCount {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .config("spark.rdd.compress", "true")
     .config("spark.speculation", "false")
     .config("spark.io.compression.codec", "snappy")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .getOrCreate()
    val input = args(0)
    val output = args(1)
    FileSystem.get(new URI(s"s3://mob-emr-test"), spark.sparkContext.hadoopConfiguration).delete(new Path(output), true)
    val sc = spark.sparkContext
    sc.textFile(input,2)
      .flatMap(line=> line.split(" "))
      .map(word=>(word,1))
      .reduceByKey((a,b)=> a+b)
      .saveAsTextFile(output)
  }
}
