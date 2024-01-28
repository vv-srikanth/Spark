package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object full_join_scenario {
def main(args:Array[String]):Unit={

System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")
val sc = new SparkContext(conf)   // RDD
sc.setLogLevel("ERROR")
val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

val src = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/source.csv")
src.show()     

val tgt = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/target.csv")
tgt.show()   

val full = src.join(tgt, Seq("id"), "outer").orderBy("id")
full.show()

val compare = full.withColumn("comment", expr("case when sname=tname then 'Match' else 'mismatch' end"))
compare.show()

val com = compare.withColumn("comment",
    expr("case when sname is null then 'New in Source' when tname is null then 'New in Target' else comment end"))
com.show()

val notmatch = com.filter(! (col("comment")==="Match"))
notmatch.show()

val finaldf = notmatch.select("id","comment")
finaldf.show()

  }
  
}
  