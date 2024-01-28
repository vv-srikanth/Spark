package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object aggregators {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly
			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD
			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe
			import spark.implicits._

			val df = spark.read.format("csv").option("header","true").load("file:///D:/zeyolabfiles/agg.csv")
			df.show()
			
val aggdf = df.groupBy("name")
			              .agg(sum("amt").cast(IntegerType).as("total"),
			                   count("amt").as("cnt")
			                  )
aggdf.show()
			
val sumResult = df.groupBy("name").agg(sum("amt").as("total_value"))
sumResult.show()

val avgResult = df.groupBy("name").agg(avg("amt").as("average_value"))
avgResult.show()

val minResult = df.groupBy("name").agg(min("amt").as("min_value"))
minResult.show()

val maxResult = df.groupBy("name").agg(max("amt").as("max_value"))
maxResult.show()

val countResult = df.groupBy("name").agg(count("amt").as("value_count"))
countResult.show()

val distinctCountResult = df.groupBy("name").agg(countDistinct("amt").as("distinct_value_count"))
distinctCountResult.show()

val firstResult = df.groupBy("name").agg(first("amt").as("first_value"))
firstResult.show()

val lastResult = df.groupBy("name").agg(last("amt").as("last_value"))
lastResult.show()

val collectListResult = df.groupBy("name").agg(collect_list("amt").as("list_of_values"))
collectListResult.show()

val collectSetResult = df.groupBy("name").agg(collect_set("amt").as("set_of_values"))
collectSetResult.show()

			
	}

}