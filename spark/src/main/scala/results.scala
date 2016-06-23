import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

import org.apache.spark.sql._

import org.apache.spark.sql.SQLContext._

import org.elasticsearch.spark._

import org.elasticsearch.spark.sql._  
object results {

 def main(args: Array[String]) {
   	val conf = new SparkConf().setAppName("tags_result")
   	//conf.set("es.resource", "tags");
        val query_str="hadoop"      
    	conf.set("es.index.auto.create", "true")
   	conf.set("es.nodes", "localhost,172.31.0.68")
   	conf.set("es.port", "9200")
   	val sc = new SparkContext(conf)
	val query_tags=tags_process.rangeTags(query_str,";")
        //import sqlContext.implicits._
        val sql = new SQLContext(sc)
        val df = sql.read.format("org.elasticsearch.spark.sql").load("ela/test4")
        df.printSchema()
        df.registerTempTable("tags_stat")
       val output_tags_stat=sql.sql("select ave_time ,tags from tags_stat where tags like '%hadoop%'").orderBy("ave_time")
        //val output_tags_stat=sql.sql("select * from tags_stat where tags like '% "+query_tags+"%'")
       //println(people.schema.treeString)
       output_tags_stat.rdd.collect.foreach(a=>println(a))
}


}
