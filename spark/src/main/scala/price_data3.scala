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

object price_data3 {

 def main(args: Array[String]) {

   case class TagsStat(tags: String, ave_time: Long, no_aws:Int,no_no_aws:Int,part_aws_in_num:Int,part_aws_no_in_num:Int)
   case class AnswerGuys(id:String,tags: mutable.HashSet[String])

   // setup the Spark Context named sc
   val format = new java.text.SimpleDateFormat("yyyy_MM_dd_kk_mm_ss")
   val conf = new SparkConf().setAppName("PriceDataExercise")
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
   if(args(0)=="1")//initial content
   { 

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

  //"ave_time"-> (a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 }),
                //"no_aws"->a._2._2,
                //"no_no_aws"->a._2._3


   println("---------------------+++++++++++++++++++++++++")
   res_f.saveToEs("spark/tags",Map("es.mapping.id" -> "tags"))
   //val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
   //val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
   //sc.makeRDD(Seq(numbers, airports)).collect.foreach(a=>(a))
   //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
   //res_f.collect.foreach(a=>println(a))
   //println(questions.count) 
   val answer_map=answers_befo_redu.map(a=>(a._1,(a._2.split(",")(5),"0")))
   val question_map=questions.map(a=>(a._1,(a._2.split(",")(4),"1")))
   val initialSet = mutable.HashSet.empty[String]
   val addToSet = (s: mutable.HashSet[String], v: String) => s += v
   val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
   val answerGuys=answer_map.join(question_map)
		.map(a=>mapAnswerGuys(a._2))
		.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
   
   }
   else{
      //arg[1]is file name,should be associated with nth day
      //val lines:RDD[String]
      val lines=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1))
      //no left content
      //if(args(2)=="0"){
       // val lines=line1
      //}
      //else{// merge left content
       /// val line2=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(2))
       /// val lines = line1.union(line2)
      //}
      
      val answers=lines.filter(l=>l.split(",")(1)=="1").map(l=>(l.split(",")(2),l)).reduceByKey((a,b)=>if(format.parse(a.split(",")(3)).getTime()<format.parse(b.split(",")(3)).getTime())a else b )
      
      val questions=lines.filter(l=>l.split(",")(1)=="0").map(l=>(l.split(",")(0),l))
     val res_q=questions.leftOuterJoin(answers)
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

     //val res_f=res_q.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,"")).map(a=>(a._1,(a._2._1,a._2._2.toLong,a._2._3.toLong)))
     val no_aws_qs=res_q.filter(a=>a._2._1==0).map(a=>a._2._4)
     no_aws_qs.saveAsTextFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/"+args(1)+"_no")
     println("**********************************************************")
     //..............define times
     val period=args(3).toLong*1000*60   //periodic time of doing the job in milisecond
     val compareTime=10*60*1000 //10 mins
     val interval=args(4)                // time window size 
     // .............define times
     val latest_no_aws_qs=no_aws_qs.filter(a=>(Math.abs(format.parse(a.split(",")(3)).getTime()-format.parse(args(1)).getTime()) < period )
	//&& 
	//(format.parse(a.split(",")(3)).getTime()+ compareTime< format.parse(args(1)).getTime())
	)
     val num_no_aws_qs_tags=latest_no_aws_qs.map(a=>(rangeTags(a.split(",")(4),";"),1)).reduceByKey((a,b)=>a+b)
     //....create database df
     val db_df = sqlContext.read.format("org.elasticsearch.spark.sql").load("spark/tags")
     //println("--+___+=_+_+_+_+_+_+_+_+_+_+_+_+_+")
     //db_df.collect.foreach(a=>println(a))
     //println("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
     //....create database df
    println("**********************************************************---------------------------------")
     val aws_qs=res_q.filter(a=>a._2._1!=0)
     val aws_qs_in_time_bef_red=aws_qs.filter(a=> a._2._1 <= compareTime)
     val aws_qs_in_time= aws_qs_in_time_bef_red.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,0,"")).map(a=>(a._1,a._2._2))
     println("**********************************************************--------------------------------0")
     val aws_qs_no_in_time=aws_qs.filter(a=> a._2._1 > compareTime).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,0,"")).map(a=>(a._1,a._2._2))
        println("**********************************************************--------------------------------1")
     val aws_qs_no_in_time_total=aws_qs_no_in_time.union(num_no_aws_qs_tags).reduceByKey((a,b)=>a+b).map(a=>(a._1,a._2)) 
     //val aws_qs_no_in_time_total=aws_qs_no_in_time.fullOuterJoin(num_no_aws_qs_tags).reduceByKey((a,b)=>a).map(a=>(a._1,a._2))
      println("**********************************************************--------------------------------2")

     //grab records for lower edge of the time window
     val last_record_t=interval
     //"2015_12_22_00_00_00" // todo : use val[interval] to get 
     val records_la=sqlContext.read.json("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/tmp/"+last_record_t+"_tmp").rdd
     //records_la.collect.foreach(a=>println(a))  
      println("**********************************************************--------------------------------3")
     val records_la_rearranged=records_la.map(a=>(a.getString(2),(a.getLong(0),a.getLong(1),0,0.toLong,0.toLong)))
       println("**********************************************************--------------------------------4")
     //save records of current time slot 
     val new_records=aws_qs_in_time.fullOuterJoin(aws_qs_no_in_time_total).map(a=>(a._1,a._2._1.getOrElse(0).toLong,a._2._2.getOrElse(0).toLong))
       println("**********************************************************--------------------------------5")
     ///new_records.toDF("tags","in_time","out_time").toJSON.saveAsTextFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/tmp/"+args(1)+"_tmp")
     val tag_change=records_la_rearranged.union(new_records.map(a=>(a._1,(a._2,a._3,0,0.toLong,0.toLong)))).reduceByKey((a,b)=>(0,0,0,a._1-b._1,a._2-b._2))
     .map(a=>(a._1,(a._2._1.toLong,a._2._2.toLong,a._2._3.toLong,a._2._4.toLong,a._2._5.toLong)))
        println("**********************************************************--------------------------------6")
     
     val aws_f=aws_qs.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,"")).map(a=>(a._1,(a._2._1,a._2._2.toLong,a._2._3.toLong,0.toLong,0.toLong)))
     println("**********************************************************---------------------------------7") 
     //....update database
     val update_db= db_df.rdd.map(a=>(a.getString(5),(a.getLong(0),a.getLong(1),a.getLong(2),a.getLong(3),a.getLong(4))))
     .union(aws_f).union(tag_change).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5))
     .map( a=>TagsStat(a._1,a._2._1,a._2._2.toInt,a._2._3.toInt,a._2._4.toInt,a._2._5.toInt))
     //update_db.collect.foreach(a=>println(a))
     println("**********************************************************---------------------------------8")
     update_db.saveToEs("spark/tags"
      ,Map("es.mapping.id" -> "tags")
      )

    // val update_db=db_df.rdd..map(a=>(a.getString(3),(a.getLong(0),a.getLong(1),a.getLong(2),a.getLong(3),a.getLong(4))))
     //.union(tag_change).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5))     
    //....update database
    


    
     

     //questions being answered and not being answered in this time slot
     //val qNoAws=res_q.filter._
  

   }  
 }
 def rangeTags(tags : String, splitter: String) : String ={
  val sortedTags=tags.split(splitter).sorted.mkString(splitter)
  return sortedTags

 }
// def mapAnswerGuys(arr :((String,String),(String,String))): Seq[(String,String)]={
//  val seq=Seq[(String,String)]()
//  var tags=""
//  for(i <- arr){
//   if(i._2=="1")
//	tags=i._1
// }
//  for(i<-arr){
//   if(i._2=="0"){
//     i._2=tags
//     seq:+i
//   }
//  }
//  return seq;
// }

def mapAnswerGuys(tup :((String,String),(String,String))): (String,String)={
  if(tup._1._2=="1"){
        return (tup._2._1,tup._1._1)
  }else{
        return (tup._1._1,tup._2._1)  
  }
}
}
