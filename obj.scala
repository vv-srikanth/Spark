package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object obj {


	def main(args:Array[String]):Unit={



			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly
			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD
			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe
			import spark.implicits._

			val df = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/dtnew.txt")
			df.show()


		
val filcat = df.filter( ! ( col("category")==="Exercise" ) )
filcat.show()
			
val filand = df.filter( !  (  col("category")==="Exercise" && col("spendby")==="cash" ))
filand.show()
			
val filor = df.filter( ! (  col("category")==="Exercise" or col("spendby")==="cash" ))
filor.show()			
			
val filin = df.filter( ! (col("category") isin ("Exercise","Team Sports")))
filin.show()
			
val fillike = df.filter( ! (col("product") like "%Gymnastics%" ))
fillike.show()
			
val filnull = df.filter(col("product") isNull) //we can use .isNull or isNull
filnull.show()
			
val fillnotnull = df.filter(col("product") isNotNull)
fillnotnull.show()
			
			
			
	}

}