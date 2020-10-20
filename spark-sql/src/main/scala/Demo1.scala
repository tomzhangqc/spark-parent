import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
//    val user = spark.read.json("/Users/kemingyuye/IdeaProjects/project/spark/data/user.json")
//    user.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
//    user.select('name, 'age).show()
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan",30), (2, "lisi", 20)))
//    val df: DataFrame = rdd.toDF("id", "name", "age")
//    val dfToRdd: RDD[Row] = df.rdd
//    dfToRdd.foreach(row => {
//      println(row(0))
//    })
    val userRdd = rdd.map({
      case (id, name, age) => {
        User(id, name, age)
      }
    })
    val userDS: Dataset[User] = userRdd.toDS()
    userDS.createOrReplaceTempView("user")
    spark.udf.register("addName",(x:String)=>"s1"+x)
    spark.sql("select addName(name),id,age from user").show()
//    val dsToDf: DataFrame = userRdd.toDF()
//    rdd.foreach(println)
//    df.show()
//    userDS.show()
    spark.stop()
  }
}

case class User(id: Int, name: String, age: Int)
