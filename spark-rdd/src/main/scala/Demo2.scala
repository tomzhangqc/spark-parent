import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3), 3)
    println(rdd.collect().mkString(","))
    sc.stop()
  }
}
