import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zhangqingchun
 * @date 2020/12/16
 * @description
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("demo3")
    val sparkContext = new SparkContext(sparkConf)
    val stringRDD = sparkContext.textFile("datas", 1)
    val value = stringRDD.flatMap(_.split(" "))
    val groupValue = value.groupBy(k => k)
    val value1 = groupValue.map(v => {
      (v._1, v._2.size)
    })
    value1.collect().foreach(println)
    sparkContext.stop()
  }
}
