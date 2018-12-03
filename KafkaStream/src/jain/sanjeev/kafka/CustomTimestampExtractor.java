package jain.sanjeev.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomTimestampExtractor  implements TimestampExtractor{
	
	@Override
	public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
		
		ObjectMapper om = new ObjectMapper();
		JsonNode rootNode = null;
		String mapData = null;
		long recordTime=0l;
	

		
		
		try {
			 mapData = (String) record.value().toString();
				
			rootNode = 	om.readTree(mapData);
			JsonNode userNode = rootNode.path("MODIFIED");
			
			String strTime = (String)userNode.asText();
			
			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
			Date d = f.parse(strTime);
			
			 recordTime = d.getTime();

			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		
		return recordTime;
		
	}

}
