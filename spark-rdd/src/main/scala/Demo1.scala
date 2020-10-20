import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sc.textFile("/Users/IdeaProjects/project/flink-demo/spark/data/scala.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = wordRDD.map(data => (data, 1))
    val wordSumRDD = mapRDD.reduceByKey(_ + _)
    val wordCount: Array[(String, Int)] = wordSumRDD.collect()
    println(wordCount.mkString(","))
    sc.stop()
  }
}
