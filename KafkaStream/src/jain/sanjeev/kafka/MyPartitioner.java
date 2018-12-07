package jain.sanjeev.kafka;

import org.apache.kafka.streams.processor.StreamPartitioner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MyPartitioner implements StreamPartitioner<JsonNode,JsonNode> {

	
	@Override
	public   Integer partition(JsonNode k, JsonNode v, int numPartitions) {
		
		ObjectMapper om = new ObjectMapper();

		int newPartition=0;
		
		try {
			
			String recordValue = (String) v.toString();
			JsonNode rootNode =  om.readTree(recordValue);
			
			
			if (!rootNode.isNull()) {
						
				// get userName from the record
				
				JsonNode userNode = rootNode.path("USERNAME");
				String userName = (String)userNode.asText().toUpperCase();
				
				
				int np= userName.hashCode() % numPartitions;
				System.out.println("number of Partions:" + numPartitions + "partition number : " + np );

	        	char  startChar = (userName.toCharArray())[0];
	        	
	       
	        	 if (startChar >=65 && startChar <= 71)
	        		 newPartition=0;
	        	else if (startChar >=72 && startChar <= 78)
	        		newPartition=1;
	        	else if(startChar >=79 && startChar <= 85)
	        		newPartition=2;
	        	else
	        		newPartition=3;
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return newPartition;
	}
}
