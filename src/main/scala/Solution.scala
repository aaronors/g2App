
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
  * Created by aaronors on 6/4/17.
  */

object Solution {

  def main(args: Array[String]) {


    /**----------------- Setup -----------------**/

    val conf = new SparkConf().setAppName("Solution").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("SQLSolution")
      .getOrCreate()

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    /**----------------- Assets -----------------**/

   /** open assets.txt file **/
    val assetsFile = sc.textFile("C:\\Users\\aaronors\\IdeaProjects\\g2App\\src\\main\\resources\\assets.txt")

    /** parse to RDD[string] of JSON lines **/
    val assetsStr = assetsFile.map(line => parseLine(line))


    /** convert lines to DF **/
    val assetsDF = spark.read.json(assetsStr)
    assetsDF.createOrReplaceTempView("assets")

    /** access using SQL statements **/
    val pageviewDF = spark.sql("SELECT DISTINCT pv, COUNT(pv) FROM assets GROUP BY pv ")


    /** output for pageviewDF100.txt **/
    //pageviewDF.take(100).foreach(println)

    /**----------------- Ad Events -----------------**/


    val adeventFile = sc.textFile("C:\\Users\\aaronors\\IdeaProjects\\g2App\\src\\main\\resources\\ad_events.txt")

    val adeventStr = adeventFile.map(line => parseLine(line))

    val adeventDF = spark.read.json(adeventStr)

    adeventDF.createOrReplaceTempView("adevent")

    /** one sql for views and one for clicks **/

    val eclickDF = spark.sql("SELECT DISTINCT pv, COUNT(pv) AS clickCnt FROM adevent WHERE e LIKE 'click' GROUP BY pv")

    /** output for eclickDF100.txt **/
   // eclickDF.take(100).foreach(println)


    val eviewDF = spark.sql("SELECT DISTINCT pv, COUNT(pv) AS viewCnt FROM adevent WHERE e LIKE 'view' GROUP BY pv")

    /** output for eviewDF100.txt **/
    // eviewDF.take(100).foreach(println)


    /**----------- Formatting and Output -----------**/

    /** Join outputs by pv**/

   /** val output = adeventDF.join(eclickDF, adeventDF("pv") === eclickDF("pv"))
      .join(eviewDF, adeventDF("pv") === eviewDF("pv"))
      .select(adeventDF("pv"),eclickDF("clickCnt"),eviewDF("viewCnt")) **/


    /** Output to text file **/


   /** val out = pageviewDF.write.save("C:\\Users\\aaronors\\IdeaProjects\\g2App\\src\\main\\output\\out1.txt") **/



  }

  /** split each line and return the last JSON**/
  def parseLine(str : String): String ={
    val strParse = str.split("\\, \\{")
    val bracket = "{"

    return bracket.concat(strParse(strParse.length-1))
  }
}


