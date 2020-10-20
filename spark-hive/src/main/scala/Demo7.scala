import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties

object Demo7 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordCount")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    //    spark.sql("create table a (id int)")
    spark.sparkContext.setLogLevel("ERROR")
    val result = spark.sql("select name,age from student")
    //    result.show()
    //    result.write.mode(SaveMode.Append).format("json").save("output.txt")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark", "student", prop)
    spark.stop()
  }
}
