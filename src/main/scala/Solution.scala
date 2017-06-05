
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._



/**
  * Created by toddmcgrath on 6/15/16.
  */
object Solution {

  def main(args: Array[String]) {


    /** Setup **/

    val conf = new SparkConf().setAppName("dataAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "C:\\winutils")



    /** open assets file **/
    val textFile = scala.io.Source.fromURL("http://gumgum-share.s3.amazonaws.com/assets_2014-01-20_00_domU-12-31-39-01-A1-34.gz").mkString

    val list = textFile.split("\n")


    list.take(2).foreach(println)




    /** split each line with comma **/





    /** create new file, parse assets file, output into new file   **/



    /**----------- Proposed Solution ------------------
      *  - open file(do for both)
      *  - extract relevant json
      *  - convert to json line file
      *---------------------------------------------**/
  }

}


/** --------------------- Notes ----------------------
  *
  * (make sure to check documentation for correct version !!!!!!!!!)
  *
  * sql context not used in 2.1.1
  *
  *
  *
  *
  *
  *
  *
  *------------------------------------------------**/



/** **/