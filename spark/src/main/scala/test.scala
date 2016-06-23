import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import org.apache.spark.SparkConf

//import org.apache.spark.sql._

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
//import redis.clients.jedis.Jedis

object test{
def main(args: Array[String]) {
val conf = new SparkConf().setAppName("PriceDataExercise")
   //conf.set("es.resource", "tags");
  
  
val sc = new SparkContext(conf)
 val lines=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(0))
 lines.collect.foreach(a=>println(a))
}

}
