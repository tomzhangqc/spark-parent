import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

object Demo5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 20)))
    val userRdd = rdd.map({
      case (id, name, age) => {
        User(id, name, age)
      }
    })
    val userDS: Dataset[User] = userRdd.toDS()
    val myAvg = new MyAvgUDAFClass
    userDS.select(myAvg.toColumn).show()
    spark.stop()
  }
}

class MyAvgUDAFClass extends Aggregator[User, AvgBuff, Int] {

  override def zero: AvgBuff = AvgBuff(0, 0)

  override def reduce(b: AvgBuff, a: User): AvgBuff = {
    AvgBuff(b.total + a.age, b.count + 1)
  }

  override def merge(b1: AvgBuff, b2: AvgBuff): AvgBuff = {
    val total = b1.total + b2.total
    val count = b1.count + b2.count
    AvgBuff(total, count)
  }

  override def finish(reduction: AvgBuff): Int = {
    reduction.total / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuff] = Encoders.product

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

case class AvgBuff(total: Int, count: Int)
