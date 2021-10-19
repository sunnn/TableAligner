import com.nar.tbl.util.TableUtil
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class UtilTest extends FunSuite with BeforeAndAfterAll{

  private val master = "local"

  private val appName = "ReadFileTest"

  var spark : SparkSession = _

  override def beforeAll(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()

    def createtestobj()={
      val spark2 = spark
      import spark2.implicits._
      val columns=Array("sno", "first", "last", "year")
      val df1=spark.sparkContext.parallelize(Seq(
        (1, "John", "Doe", 1986),
        (2, "Ive", "Fish", 1990),
        (4, "John", "Wayne", 1995)
      )).toDF(columns: _*)

      val columns1=Array("id", "first", "last", "year","population","country")
      val df2=spark.sparkContext.parallelize(Seq(
        (1, "John", "Doe", 1986,23,"IND"),
        (2, "IveNew", "Fish", 1990,45,"CHN"),
        (3, "San", "Simon", 1974,67,"USA")
      )).toDF(columns1: _*)

      df1.write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/user/narman/source")
      df2.write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/user/narman/target")
    }

    createtestobj()
  }

  test("sample "){
    TableUtil.apply_config(spark,"src/test/resources/tableprops.json")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
