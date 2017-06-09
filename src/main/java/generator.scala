import db_creation.sqlContext

/**
  * Created by zurbzh on 2017-06-09.
  */
class generator (db_file:String) {
  val df = db_creation.receive(db_file)
  df.registerTempTable("data")

  val unique_indicator = df.select("indicator").collect().distinct.toList.map(r => r.getInt(0))
  val unique_year = df.select("index_year").collect().distinct.toList.map(r => r.getString(0))
  val unique_age = df.select("age").collect().distinct.toList.map(r => r.getInt(0))
  val unique_age_sorted = unique_age.sortWith(_ < _)

  def parseDouble(value: String) = try { Some(value.toDouble) } catch { case _ => None }

  def sql_command_indicators (ind:Int, age:(Int,Int), year:String):Double = {

    val command = s"SELECT Round(SUM(numerator)/SUM(denominator)*100, 2) FROM data WHERE indicator = $ind AND age >= ${age._1} AND age <= ${age._2} AND index_year = $year"
    val retrieved_value = Option(sqlContext.sql(command).first().get(0)) match {
      case Some(v) => v
      case _ => 0.0
    }
    val result = parseDouble(retrieved_value.toString) match {
      case Some(v) => v
    }
    return result
  }

  def sql_command_age (year:String, age:Int, Ind:Int):Double = {

    val command = s"SELECT Round(SUM(numerator)/SUM(denominator)*100, 2) FROM data WHERE index_year = $year AND age = $age AND indicator = $Ind"
    val retrieved_value = Option(sqlContext.sql(command).first().get(0)) match {
      case Some(v) => v
      case _ => 0.0
    }
    val result = parseDouble(retrieved_value.toString) match {
      case Some(v) => v
    }
    return result
  }



  def generator_X_indicators(file_location: String) = {

    val ages = List((25,29),(30,34),(35,39),(40,44),(45,49),(50,54),(55,59),(30,59))
    val row_names = List("25-29","30-34","35-39","40-44","45-49","50-54","55-59", "All ages: 30-59 years")
    val full_column_names = List("indicator","1","2.5","3.5","4.5","5.5","6.5","7.5","8.5","9.5","10.5")

    for (year <- unique_year) {
          //query to retrieve values from db
          val lists_from_sql = unique_indicator.map(ind => ages.map(age => sql_command_indicators(ind, age, year)))
          // delete lists which contain only 0s
          val cleaned_lists_from_sql = for (ls <- lists_from_sql if ls.reduceLeft(_ + _) > 0 ) yield ls
          // add row and column names according to the number of retrieved lists
          val number_of_columns = cleaned_lists_from_sql.length + 1
          val column_names = full_column_names.take(number_of_columns)
          val lists_with_row_names = row_names :: cleaned_lists_from_sql
          val indicators_data = lists_with_row_names.transpose
          val ready_to_write = column_names :: indicators_data

          // name each file according to the year for which the values were retrieved
          val txt_name = year.toString + ".txt"
          val location = file_location
          val txt = location.concat(txt_name)

          val fw = new FileWriter(txt, true)
          ready_to_write.foreach(x => fw.write(x.mkString(",") + "\n"))
          fw.close()

    }
}

  /*def generator_X_age(file_location: String) = {

    val column_list = List(unique_age_sorted.min to unique_age_sorted.max)
    val column_names = "Ages" :: column_list
    val full_row_names = List("Follow-up time: 1 year", "Follow-up time: 2 years", "Follow-up time: 3 years", "Follow-up time: 4 years", "Follow-up time: 5 years", "Follow-up time: 6 years", "Follow-up time: 7 years", "Follow-up time: 8 years", "Follow-up time: 9 years", "Follow-up time: 10 years")


    val lists_from_sql = unique_age_sorted.map(age => unique_indicator.map(ind => sql_command_age("1995", age, ind)))
    val age_data = lists_from_sql.transpose
    val cleaned_age_data = for (ls <- age_data if ls.reduceLeft(_ + _) > 0 ) yield ls
    println(cleaned_age_data)
    val row_names = full_row_names.take(cleaned_age_data.length)
    println(row_names)
    val named_age_data = cleaned_age_data.map(ls => row_names.map(name => name::ls))

    cleaned_age_data.foreach(x => println(x))
  } */
}
