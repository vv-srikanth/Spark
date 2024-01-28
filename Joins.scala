package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object joins {

def main(args:Array[String]):Unit={

System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")
val sc = new SparkContext(conf)   // RDD
sc.setLogLevel("ERROR")
val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe
import spark.implicits._

val cust = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/custn.csv")
cust.show()   

val prod = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/prodn.csv")
prod.show() 

val left = cust.join(prod, Seq("id"), "left").orderBy("id") ///left----left_outer----leftouter
left.show()

val lefto = cust.join(prod, Seq("id"), "leftouter").orderBy("id")
lefto.show()

val right = cust.join(prod, Seq("id"), "right").orderBy("id")
right.show()

val righto = cust.join(prod, Seq("id"), "right_outer").orderBy("id")
righto.show()

val full = cust.join(prod, Seq("id"), "full").orderBy("id")
full.show()

val inner = cust.join(prod, Seq("id"), "inner").orderBy("id")
inner.show()

val leftanti = cust.join(prod, Seq("id"), "left_anti").orderBy("id")
leftanti.show()

val leftsemi = cust.join(prod, Seq("id"), "left_semi").orderBy("id")
leftsemi.show()

}
}