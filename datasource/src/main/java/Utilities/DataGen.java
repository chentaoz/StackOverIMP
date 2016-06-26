package Utilities;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;

public class DataGen {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			dataGen();
			System.out.println(1);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void dataGen() throws UnknownHostException, InterruptedException, ExecutionException {
		// on startup

		Client client = TransportClient.builder().build()
			
				 .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ec2-52-41-44-96.us-west-2.compute.amazonaws.com"), 9300))
				 .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ec2-52-41-16-102.us-west-2.compute.amazonaws.com"), 9300))
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ec2-52-40-160-114.us-west-2.compute.amazonaws.com"), 9300))
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ec2-52-40-120-253.us-west-2.compute.amazonaws.com"), 9300));

		// on shutdown
		UpdateRequest updateRequest = new UpdateRequest("spark", "tags", "invision-power-board;svn-reintegrate")
		        .script(new Script("ctx._source.no_no_aws =5"));
		client.update(updateRequest).get();
		client.close();
	}
}
