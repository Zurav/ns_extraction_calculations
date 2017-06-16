/**
  * Created by zurbzh on 2017-06-09.
  */
object main {

  def main(args: Array[String]) {

    val db = "/Users/zurbzh/Desktop/Nordscreen_testrun_Norway/coverage_table_v4_NOR_clean_over89excluded.csv"
    val file_location = "/Users/zurbzh/Desktop/Nordscreen_testrun_Norway/test/"
    val gen = new generator(db)
    //gen.generator_X_indicators(file_location)
    //gen.generator_X_age(file_location)
    gen.generator_X_year(file_location)

  }

}
