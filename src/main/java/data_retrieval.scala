import db_creation.sqlContext

/**
  * Created by zurbzh on 2017-06-13.
  */
object data_retrieval {

  def parseDouble(value: String) = try { Some(value.toDouble) } catch { case _ => None }

  def sql_command_indicators (ind:Int, age:(Int,Int), year:Int):Double = {

    val command = s"SELECT Round(SUM(numerator)/SUM(denominator)*100, 1) FROM data WHERE indicator = $ind AND age >= ${age._1} AND age <= ${age._2} AND index_year = $year"
    val retrieved_value = Option(sqlContext.sql(command).first().get(0)) match {
      case Some(v) => v
      case _ => 0.0
    }
    val result = parseDouble(retrieved_value.toString) match {
      case Some(v) => v
    }
    return result
  }



  def sql_command_age (year:Int, age:Int, Ind:Int):Double = {

    val command = s"SELECT Round(SUM(numerator)/SUM(denominator)*100, 1) FROM data WHERE index_year = $year AND age = $age AND indicator = $Ind"
    val retrieved_value = Option(sqlContext.sql(command).first().get(0)) match {
      case Some(v) => v
      case _ => 0.0
    }
    val result = parseDouble(retrieved_value.toString) match {
      case Some(v) => v
    }
    return result
  }
}
