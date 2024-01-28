package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object monotonically_scenario {


def main(args:Array[String]):Unit={
System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")
val sc = new SparkContext(conf)   // RDD
sc.setLogLevel("ERROR")

val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe
import spark.implicits._


val lis1 = List( "1" , "2" , "3")
val list2 = List( "one" , "two" , "three" )

val lis1df = lis1.toDF("nid")
lis1df.show()

val lis2df = list2.toDF("nname")
lis2df.show()

val lis1in = lis1df.withColumn("cid",monotonically_increasing_id())
lis1in.show()

val lis2in = lis2df.withColumn("cid",monotonically_increasing_id())
lis2in.show()

val joindf = lis1in.join(lis2in,Seq("cid"),"inner")
joindf.show()

val condf = joindf.withColumn("value",expr("concat(nid, ' is ',nname)"))
condf.show()

condf.select("value").show()


}

}