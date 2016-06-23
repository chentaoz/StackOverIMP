import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import org.apache.spark.SparkConf

//import org.apache.spark.sql._

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._  
object price_data {

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
   val lines=sc.textFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/fakedata")
   //val lines=sc.textFile("~/test.txt") 
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
     }
    ))
   )
  val res_f=res_q.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3))
      .map(
	a=>TagsStat(a._1,(a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 }).toLong,a._2._2,a._2._3)
	   //(
		//a._1,(
	        //a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 },
                //a._2._2,
                //a._2._3
               	//)
               
	  //)
	)
    
  //"ave_time"-> (a._2._2 match {case 0=> 0 case _ => a._2._1/a._2._2 }),
                //"no_aws"->a._2._2,
                //"no_no_aws"->a._2._3   
   

   println("---------------------+++++++++++++++++++++++++")
   res_f.saveToEs("ela/test5"
   ,Map("es.mapping.id" -> "tags")
   )
   //val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
   //val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
   //sc.makeRDD(Seq(numbers, airports)).collect.foreach(a=>(a))
   //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
   res_f.collect.foreach(a=>println(a))
   println(questions.count)	
   //val sc: SparkContext
   //val sqlContext = new SQLContext(sc) 
   // read data from csv file
   //import sqlContext.implicits._
   //val df = sqlContext.read.format("json").load("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/data/QueryResult3.json")	
   // folder on HDFS to pull the data from
   //df.show()
   //df.printSchema()
   //df.select("_id").show()
   // val rdd = sc.cassandraTable("spark", "try")
   //val file_collect=rdd.collect()
    //file_collect.map(println(_))
  
    //val selectedData = df.select("Id", "PostType","ParentId","CreationDate")                                      
    //selectedData.write
    //.format("com.databricks.spark.csv")
    //.option("header", "true")
    //.save("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/test_data_output_scala/test.csv")
   // save the data back into HDFS

   //price_vol_min30.saveAsTextFile("hdfs://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala")

 }
 def rangeTags(tags : String, splitter: String) : String ={
  val sortedTags=tags.split(splitter).sorted.mkString(splitter)
  return sortedTags
  
 }

}
