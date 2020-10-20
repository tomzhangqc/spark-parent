import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Demo4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 20)))
    val userRdd = rdd.map({
      case (id, name, age) => {
        User(id, name, age)
      }
    })
    val userDS: Dataset[User] = userRdd.toDS()
    userDS.createOrReplaceTempView("user")
    val myAvg = new MyAvg()
    spark.udf.register("myAvg", myAvg)
    spark.sql("select myAvg(age) from user").show()
    spark.stop()
  }
}

class MyAvg extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    StructType(Array(StructField("age", IntegerType)))
  }

  override def bufferSchema: StructType = {
    StructType(Array(StructField("totalAge", IntegerType), StructField("count", IntegerType)))
  }

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0) / buffer.getInt(1)
  }
}