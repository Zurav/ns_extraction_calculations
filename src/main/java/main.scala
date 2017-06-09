/**
  * Created by zurbzh on 2017-06-09.
  */
object main {

  def main(args: Array[String]) {

    val db = "/Users/zurbzh/Desktop/Nordscreen_testrun_Finland/nscr1_agg_data_export_20161102.csv"
    val file_location = "/Users/zurbzh/Desktop/Nordscreen_testrun_Finland/test/"
    val gen = new generator(db)
    //gen.generator_X_indicators(file_location)
    gen.generator_X_age(file_location)

  }

}
