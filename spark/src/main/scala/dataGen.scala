import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
//import org.apache.spark.sql.SQLContext.implicits._
import org.apache.spark.SparkConf

//import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
//import redis.clients.jedis.Jedis
import scala.util.Random
//import org.scala_tools.time.Imports._
object dataGen {

 def main(args: Array[String]) {
   case class TagsStat(tags: String, ave_time: Long, no_aws:Int,no_no_aws:Int,part_aws_in_num:Int,part_aws_no_in_num:Int)
   val format = new java.text.SimpleDateFormat("yyyy_MM_dd_kk_mm_ss")
   val conf = new SparkConf().setAppName("datagen")
   //conf.set("es.resource", "tags");
  
   conf.set("es.index.auto.create", "true")
   conf.set("es.nodes", "localhost,172.31.0.68")
   conf.set("es.port", "9200")
   conf.set("spark.core.connection.ack.wait.timeout","600")
   conf.set("spark.storage.memoryFraction","1")
   conf.set("spark.rdd.compress","true")
   conf.set("spark.akka.frameSize","50")
   val sc = new SparkContext(conf)
   //import sqlContext.implicits._
   val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._
   
   val db_df = sqlContext.read.format("org.elasticsearch.spark.sql").load("spark/tags")
   db_df.registerTempTable("Tags")
   var upDF = sqlContext.sql("select tags,ave_time,no_aws,no_no_aws,part_aws_in_num,part_aws_no_in_num from Tags")
   //upDF.show()
   // println(rand())
   // println(rand(100))
    upDF.rdd.
       map(x=>
        TagsStat(x.getAs[String]("tags"),x.getAs[Long]("ave_time")
 	,x.getAs[Long]("no_aws").toInt+(new java.util.Date()).getTime().toInt%30000
	,x.getAs[Long]("no_no_aws").toInt.toInt+(new java.util.Date()).getTime().toInt%1000
	,x.getAs[Long]("part_aws_in_num").toInt+(new java.util.Date()).getTime().toInt%3031
	,x.getAs[Long]("part_aws_no_in_num").toInt+(new java.util.Date()).getTime().toInt%4790
)
).saveToEs("spark/tags",Map("es.mapping.id" -> "tags"))
   //var ver=sqlContext.sql("select tags,ave_time,no_aws,no_no_aws,part_aws_in_num,part_aws_no_in_num from Tags WHERE tags like 'java%'")
   //ver.show()
}}
