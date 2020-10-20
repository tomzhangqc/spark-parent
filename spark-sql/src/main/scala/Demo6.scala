import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Demo6 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //    val dataFrame:DataFrame=spark.read.format("json").load("/Users/kemingyuye/IdeaProjects/project/spark/data/user.json")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    val dataFrame: DataFrame = spark.read.format("json").jdbc("jdbc:mysql://localhost:3306/spark", "student", prop)
    dataFrame.createOrReplaceTempView("student")
    spark.sql("select max(id) from student").show()
//    dataFrame.write.mode(SaveMode.Append).format("json").save("output")
    spark.stop()
  }
}
