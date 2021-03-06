import java.io.FileWriter

import db_creation.sqlContext

/**
  * Created by zurbzh on 2017-06-09.
  */
class generator (db_file:String) {
  val df = db_creation.receive(db_file)
  df.registerTempTable("data")

  def writing_txt(number:Int, location:String, matrix:List[List[Any]]): Unit = {
    val txt_name = number.toString + ".txt"
    val file_location = location
    val txt = file_location.concat(txt_name)

    val fw = new FileWriter(txt, true)
    matrix.foreach(x => fw.write(x.mkString(",") + "\n"))
    fw.close()

  }


  val unique_indicator = df.select("indicator").collect().distinct.toList.map(r => r.getInt(0))
  val unique_year = df.select("index_year").collect().distinct.toList.map(r => r.getInt(0))
  val unique_age = df.select("age").collect().distinct.toList.map(r => r.getInt(0))
  val unique_age_sorted = unique_age.sortWith(_ < _)
  val unique_year_sorted = unique_year.sortWith(_ < _)



  def generator_X_indicators(file_location: String) = {

    val ages = List((25,29),(30,34),(35,39),(40,44),(45,49),(50,54),(55,59),(30,59))
    val row_names = List("25-29","30-34","35-39","40-44","45-49","50-54","55-59", "All ages: 30-59 years")
    val full_column_names = List("indicator","1","2.5","3.5","4.5","5.5","6.5","7.5","8.5","9.5","10.5")

    for (year <- unique_year) {
          //query to retrieve values from db
          val lists_from_sql = unique_indicator.map(ind => ages.map(age => data_retrieval.sql_command_indicators(ind, age, year)))
          // delete lists which contain only 0s
          val cleaned_lists_from_sql = for (ls <- lists_from_sql if ls.reduceLeft(_ + _) > 0 ) yield ls
          // add row and column names according to the number of retrieved lists
          val number_of_columns = cleaned_lists_from_sql.length + 1
          val column_names = full_column_names.take(number_of_columns)
          val lists_with_row_names = row_names :: cleaned_lists_from_sql
          val indicators_data = lists_with_row_names.transpose
          val ready_to_write = column_names :: indicators_data

          // name each file according to the year for which the values were retrieved
          writing_txt(year, file_location, ready_to_write)

    }
  }

  def generator_X_age(file_location: String) = {

    val column_list = (unique_age_sorted.min to unique_age_sorted.max).toList
    val column_names = "Ages" :: column_list
    val full_row_names = List("Follow-up time: 1 year", "Follow-up time: 2 years", "Follow-up time: 3 years", "Follow-up time: 4 years", "Follow-up time: 5 years", "Follow-up time: 6 years", "Follow-up time: 7 years", "Follow-up time: 8 years", "Follow-up time: 9 years", "Follow-up time: 10 years")

    for (year <- unique_year) {
      val lists_from_sql = unique_age_sorted.map(age => unique_indicator.map(ind => data_retrieval.sql_command_age(year, age, ind)))
      val age_data = lists_from_sql.transpose
      val cleaned_age_data = for (ls <- age_data if ls.reduceLeft(_ + _) > 0) yield ls
      val row_names = full_row_names.take(cleaned_age_data.length)


      val age_data_row_names = for ((ls, index) <- cleaned_age_data.zipWithIndex) yield row_names(index) :: ls

      val ready_age_data = column_names :: age_data_row_names


      writing_txt(year, file_location, ready_age_data)
    }
  }




  def generator_X_year(file_location: String) = {
    val column_list = (unique_year_sorted.min to unique_year_sorted.max).toList

    val ages = List((25,29),(30,34),(35,39),(40,44),(45,49),(50,54),(55,59),(30,59))
    val full_row_names = List("25-29","30-34","35-39","40-44","45-49","50-54","55-59", "All ages: 30-59 years")


    for (ind <- unique_indicator) {

      val lists_from_sql = unique_year.map(year => ages.map(age => data_retrieval.sql_command_indicators(ind, age, year)))

      //val tr_lists_from_sql = lists_from_sql.transpose
      val cleaned_lists_from_sql = for (ls <- lists_from_sql if ls.reduceLeft(_ + _) > 0) yield ls
      val transposed_lists = cleaned_lists_from_sql.transpose
      val year_data = for (ls <- transposed_lists if ls.reduceLeft(_ + _) > 0) yield ls

      val n_exc_rows = transposed_lists.length - year_data.length
      val row_names = full_row_names.drop(n_exc_rows)

      val number_for_columns = column_list.drop(lists_from_sql.length - cleaned_lists_from_sql.length)

      val column_names = "year" :: number_for_columns

      val year_data_row_names = for ((ls, index) <- year_data.zipWithIndex) yield row_names(index) :: ls
      val ready_to_write = column_names :: year_data_row_names


      writing_txt(ind, file_location, ready_to_write)
    }
  }
}
