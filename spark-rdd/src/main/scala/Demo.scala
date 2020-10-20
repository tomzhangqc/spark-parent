import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sc.textFile("/Users/IdeaProjects/project/flink-demo/spark/data/scala.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(w => w)
    val mapRDD: RDD[(String, Int)] = groupRDD.map({
      case (word, iter) => {
        (word, iter.size)
      }
    })
    val wordCount: Array[(String, Int)] = mapRDD.collect()
    println(wordCount.mkString(","))
    sc.stop()
  }
}
