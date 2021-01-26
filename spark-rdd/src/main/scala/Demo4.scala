import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author zhangqingchun
 * @date 2021/1/5
 * @description
 */
object Demo4 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo4")
    val context = new SparkContext(sparkConf)
    val stringValue: RDD[String] = context.textFile("datas/1.txt")
    val value = stringValue.flatMap(_.split(" "))
    value.cache()
//    value.persist(StorageLevel.DISK_ONLY_2)
//    value.checkpoint()
    val value1 = value.map((_, 1))
    println(value.toDebugString)
    println(value1.dependencies)
    value1.reduceByKey(new HashPartitioner(3),_ + _).collect().foreach(println)
    context.stop()
  }
}
