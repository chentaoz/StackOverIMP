import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import org.apache.spark.SparkConf

//import org.apache.spark.sql._

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
//import com.redislabs.provider.redis._
//import com.redislabs.provider.redis._
object price_data2 {

 def main(args: Array[String]) {

   case class TagsStat(tags: String, ave_time: Long, no_aws:Int,no_no_aws:Int)

   // setup the Spark Context named sc
   val format = new java.text.SimpleDateFormat("yyyy-MM-dd kk:mm:ss")
   val conf = new SparkConf().setAppName("PriceDataExercise")
   //conf.set("es.resource", "tags");
  
   conf.set("es.index.auto.create", "true")
   conf.set("es.nodes", "localhost,172.31.0.68")
   conf.set("es.port", "9200")
   val sc = new SparkContext(conf)
   //import sqlContext.implicits._
   val sqlContext = new SQLContext(sc)
   if(args(0)=="1")//initial content
   { 

   val lines=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1))
   val answers=lines.filter(l=>l.split(",")(1)=="1").map(l=>(l.split(",")(2),l)).reduceByKey((a,b)=>if(format.parse(a.split(",")(3)).getTime()<format.parse(b.split(",")(3)).getTime())a else b )
   val questions=lines.filter(l=>l.split(",")(1)=="0").map(l=>(l.split(",")(0),l))
   val res_q=questions.leftOuterJoin(answers)
   .map(
   // a=>a._2
    a=>(rangeTags(a._2._1.split(",")(4),";"),(a._2._2 match {

        case Some(s)=> Math.abs(format.parse(a._2._1.split(",")(3)).getTime()-format.parse(s.split(",")(3)).getTime())/1000
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

  val no_aws_qs=res_q.filter(a=>a._2._1==0).map(a=>a._2._4).saveAsTextFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1)+"_no_aws")

  val res_f=res_q.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,""))
      .map(
        a=>TagsStat(a._1,(a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 }).toLong,a._2._2,a._2._3)
        )

  //"ave_time"-> (a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 }),
                //"no_aws"->a._2._2,
                //"no_no_aws"->a._2._3


   println("---------------------+++++++++++++++++++++++++")
   //res_f.saveToEs("ela/test5"
   //,Map("es.mapping.id" -> "tags")
   //)
   //val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
   //val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
   //sc.makeRDD(Seq(numbers, airports)).collect.foreach(a=>(a))
   //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
   res_f.collect.foreach(a=>println(a))
   println(questions.count)
   }
   else{
      //arg[1]is file name,should be associated with nth day
      val line1=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1))
      //no left content
      if(args(2)=="0"){
        val lines=line1
      }
      else{// merge left content
        val line2=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(2))
        val lines = line1.union(line2)
      }
      
     // val answers=lines.filter(l=>l.split(",")(1)=="1").map(l=>(l.split(",")(2),l)).reduceByKey((a,b)=>if(format.parse(a.split(",")(3)).getTime()<format.parse(b.split(",")(3)).getTime())a else b )
      
      //val questions=lines.filter(l=>l.split(",")(1)=="0").map(l=>(l.split(",")(0),l))
     //val res_q=questions.leftOuterJoin(answers)
     
     //questions being answered and not being answered in this time slot
     //val qNoAws=res_q.filter._


   }  
 }
 def rangeTags(tags : String, splitter: String) : String ={
  val sortedTags=tags.split(splitter).sorted.mkString(splitter)
  return sortedTags

 }

}
