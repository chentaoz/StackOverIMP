package Kafka;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.storm.command.list;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;

import com.csvreader.CsvReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
public class PostsProducer {

	public static void main(String[] args) {
		
		try {
			activitiesProducer();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//testProducer();
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public static void activitiesProducer() throws InterruptedException, ParseException, IOException{
		CsvReader products = new CsvReader("/Users/chentaoz/Documents/workspace/soimprove/src/test/resources/QueryResults-2.csv");
		products.readHeaders();
		ArrayList<String> tags=new ArrayList<String>();
		while (products.readRecord())
		{ 
			tags.add(products.get("TagName"));
		}
		
		Properties props = new Properties();
	        props.put("metadata.broker.list", "ec2-52-41-44-96.us-west-2.compute.amazonaws.com:9092,ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9092");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        //props.put("partitioner.class", "example.producer.SimplePartitioner");
	        props.put("request.required.acks", "1");
	 
	        ProducerConfig config = new ProducerConfig(props);
	 
	        Producer<String, String> producer = new Producer<String, String>(config);
	    

		Random rand = new Random();

		ArrayList<Long> question=new ArrayList<Long>();
		boolean flag=false;
		String date="2015_01_06_00_00_00";
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		Date d = dateFormat.parse(date);
		long postid=617209;
		String fid="2015_01_07_00_00_00";
		PrintWriter out = new PrintWriter("/Users/chentaoz/Desktop/folder1/testdata"+fid+".txt");
		PrintWriter log = new PrintWriter("/Users/chentaoz/Desktop/folder1/log/testdata"+fid+".txt");

		Calendar now = Calendar.getInstance();
	    now.setTime(dateFormat.parse(fid));
		Calendar givenDate = Calendar.getInstance();
		givenDate.setTime(d);

		boolean isBefore = now.before(givenDate); 
		String log_time="";
		while(true && givenDate.before(now)){
			
			//Thread.sleep(200);
			if(question.size()>500003)
				question.clear();
			
			HashSet<Long> randnum=new HashSet<Long>();
			
			String p_tags="";
			int count =rand.nextInt(5);
			flag=!flag;
			long parent=0;
			String type;
			if(!flag && question.size()>1){
				parent=question.get(rand.nextInt(question.size()-1));
				type="1";
				p_tags="";
			}else{
				question.add(postid);
				type="0";
				while(count>=0){
					long num=rand.nextInt(tags.size()-1);
					if(randnum.contains(num))
						continue;
					if(count==0)
						p_tags+=tags.get((int)num);
					else
						p_tags+=tags.get((int)num)+";";
					randnum.add(num);
					count--;
				}
			}
			
			
			String s_parent=""+parent;
			d.setTime(d.getTime() + rand.nextInt(15)*100);
			String time=dateFormat.format(d);
			String msg=postid+++","+type+","+s_parent+","+time+","+p_tags+","+rand.nextInt();
			System.out.println(msg);
			 log_time=time;
			 out.println(msg);
			 givenDate.setTime(d);
			
		}
		System.out.println(log_time +"  " +postid);
		log.println(log_time+" "+postid );
	}

}
