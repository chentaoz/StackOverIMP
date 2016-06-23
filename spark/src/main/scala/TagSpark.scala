import org.apache.spark.SparkContext
import scala.collection.mutable
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
//import org.apache.spark.sql.SQLContext.implicits._
import org.apache.spark.SparkConf

//import org.apache.spark.sql._

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
//import redis.clients.jedis.Jedis

object TagsSpark {

  def main(args: Array[String]) {

     case class TagsStat(tags: String, ave_time: Long, no_aws:Int,no_no_aws:Int,part_aws_in_num:Int,part_aws_no_in_num:Int)
     case class AnswerGuys(id:String,tags: mutable.HashSet[String])

     // setup the Spark Context named sc
     val format = new java.text.SimpleDateFormat("yyyy_MM_dd_kk_mm_ss")
     val conf = new SparkConf().setAppName("InsighProject")

       
     conf.set("es.index.auto.create", "true")
     conf.set("es.nodes", "localhost,172.31.0.68")
     conf.set("es.port", "9200")
     conf.set("spark.core.connection.ack.wait.timeout","600")
     conf.set("spark.storage.memoryFraction","1")
     conf.set("spark.rdd.compress","true")

     val sc = new SparkContext(conf)
     //import sqlContext.implicits._
     val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    

     val lines=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1))
     val answers_befo_redu=lines.filter(l=>l.split(",")(1)=="1").map(l=>(l.split(",")(2),l))
     val answers=answers_befo_redu.reduceByKey((a,b)=>if(format.parse(a.split(",")(3)).getTime()<format.parse(b.split(",")(3)).getTime())a else b )
     val questions=lines.filter(l=>l.split(",")(1)=="0").map(l=>(l.split(",")(0),l))
     val res_q=questions.leftOuterJoin(answers).filter(a=>a._2._1.split(",").length>=5)
     .map(
     // a=>a._2
      a=>(rangeTags(a._2._1.split(",")(4),";"),(a._2._2 match {

          case Some(s)=> Math.abs(format.parse(a._2._1.split(",")(3)).getTime()-format.parse(s.split(",")(3)).getTime())
          case None => 0
      },
       a._2._2 match {
          case Some(s) => 1
          case None =>0
       },
       a._2._2 match {
          case Some(s) => 0
          case None =>1
       },
       a._2._1
      ))
     )

    val no_aws_qs=res_q.filter(a=>a._2._1==0).map(a=>a._2._4).saveAsTextFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1)+"_no")

    val res_f=res_q.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,""))
        .map(
            a=>TagsStat(a._1,a._2._1,a._2._2,a._2._3,0,0)
          )

 


   //println("---------------------+++++++++++++++++++++++++")
   res_f.saveToEs("spark/tags",Map("es.mapping.id" -> "tags"))
   val answer_map=answers_befo_redu.map(a=>(a._1,(a._2.split(",")(5),"0")))
   val question_map=questions.map(a=>(a._1,(rangeTags(a._2.split(",")(4),";"),"1")))
   val initialSet = mutable.HashSet.empty[String]
   val addToSet = (s: mutable.HashSet[String], v: String) => s += v
   val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
   val answerGuys=answer_map.join(question_map)
		.map(a=>mapAnswerGuys(a._2))
		.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
   answerGuys.saveToEs("spark/guys",Map("es.mapping.id" -> "id"))
         
   
   
  }

   def rangeTags(tags : String, splitter: String) : String ={
    val sortedTags=tags.split(splitter).sorted.mkString(splitter)
    return sortedTags

   }


  def mapAnswerGuys(tup :((String,String),(String,String))): (String,String)={
    if(tup._1._2=="1"){
          return (tup._2._1,tup._1._1)
    }else{
          return (tup._1._1,tup._2._1)  
    }
  }
}
