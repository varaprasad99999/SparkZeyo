package SparkPack
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

object SparkObj {
  
	  def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
       val df1 = spark.read.format("json").option("multiLine","true").load("file:///F:/data/json/complexg.json")
     df1.show(false)
     df1.printSchema()

					
					
					
					
					
	  }
}