package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object kafka_streaming {



	def main(args:Array[String]):Unit={
	  System.setProperty("hadoop.home.dir", "D:\\hadoop")
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")


					val sc = new SparkContext(conf)

					sc.setLogLevel("Error")

					val spark = SparkSession
					.builder()
					.getOrCreate()
					import spark.implicits._


					val ssc = new StreamingContext(conf,Seconds(2))


					val topic = Array("nks")


					val kafkaParams = Map[String, Object](
					    "bootstrap.servers" -> "localhost:9092"
					    ,"key.deserializer" -> classOf[StringDeserializer],
							"value.deserializer" -> classOf[StringDeserializer]
							,"group.id" -> "example",
							"auto.offset.reset" -> "earliest"
							)


                   val stream = KafkaUtils.createDirectStream[String, String](
                           ssc,
                           PreferConsistent,
                           Subscribe[String, String](
                           topic, 
                           kafkaParams))
                           .map( x => x.value())
							
						
                           
							
						stream.print()

						
		
						
						ssc.start()
						ssc.awaitTermination()
							




	}

}
