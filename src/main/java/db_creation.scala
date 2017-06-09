import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
  * Created by zurbzh on 2017-06-09.
  */
object db_creation {

  val sparkConf = new SparkConf()
    .setAppName("ClusterScore")
    .setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)



  def receive (file_name:String):DataFrame = {

    val schema = new StructType()
      .add(StructField("creat_date", StringType, true))
      .add(StructField("country", StringType, true))
      .add(StructField("indicator", IntegerType, true))
      .add(StructField("gender", StringType, true))
      .add(StructField("index_year", StringType, true))
      .add(StructField("age", IntegerType, true))
      .add(StructField("denominator", LongType, true))
      .add(StructField("numerator", LongType, true))


    val csv = sc.textFile(file_name)
    val rows = csv.map(line => line.split(",").map(_.trim))
    val header = rows.first
    val data = rows.filter(_ (0) != header(0))
    val rdd = data.map(row => Row(row(0), row(1), row(2).toInt, row(3), row(4), row(5).toInt, row(6).toLong, row(7).toLong))

    val df = sqlContext.createDataFrame(rdd, schema)
    return df

  }


}
