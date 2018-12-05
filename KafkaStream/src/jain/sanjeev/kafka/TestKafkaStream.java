package jain.sanjeev.kafka;


import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;



public class TestKafkaStream {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MyKafkaStreaTestt ");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		
	    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
	    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
	    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
	    
	    String dupStateStoreName = "DupStore";
		
		StreamsConfig streamingConfig = new StreamsConfig(props);
	System.out.println("step1");
		
		StreamsBuilder builder = new StreamsBuilder();
		/**** Creating State Store  ***********/
		
		KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(dupStateStoreName); 
		
		StoreBuilder<KeyValueStore<String, String>> storeBuilder  = 
				Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.String());
		
		builder.addStateStore(storeBuilder);
		System.out.println("step2");
		
		/**** Finished Creating State Store  ***********/
		
		
		// this will read the messages from Kafka Topic connect-test
		KStream<JsonNode, JsonNode> simpleFirstStream = 
				builder.stream("connect-test",Consumed.with(jsonSerde, jsonSerde));
		System.out.println("step3");
		
		// Transforms the message to different message with updated values
	//	KStream<JsonNode, JsonNode> updateStream = simpleFirstStream.mapValues(TestKafkaStream::processMessage);
	
		KStream<JsonNode, JsonNode> updateStream = simpleFirstStream
				.transformValues(()-> new MyTransformer(dupStateStoreName),dupStateStoreName)
				.filter(TestKafkaStream::filterRecord);
		System.out.println("step4");
		
		// writing to output - topic
		
		updateStream.to( "testout",Produced.with(jsonSerde, jsonSerde));
		System.out.println("step5");
		
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
		System.out.println("step6");
		
		
		kafkaStreams.start();
		System.out.println("step7");
		
		
		try {
			
			System.out.println("Going to sleep...");
		Thread.sleep(600000);
	} catch(Exception e) {
		e.printStackTrace();
	}
		
		kafkaStreams.close();
	}// end of main

	public static JsonNode processMessage(JsonNode j) {
		
		
		ObjectMapper om = new ObjectMapper();
		JsonNode rootNode = null;
		String mapData = null;
	
		try {
			
			 mapData = (String) j.toString();
		
			rootNode = 	om.readTree(mapData);
			
			if (!rootNode.isNull()) {
						
				JsonNode userNode = rootNode.path("USERNAME");
			
			StringBuilder sb = new StringBuilder((String)userNode.asText());
			sb.append("Double");

			((ObjectNode)rootNode).put("USERNAME", sb.toString());
			
			}
		
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if(!rootNode.isNull())
		return rootNode;
		else return j;
	} // end ProcessMessgae
	
	static boolean filterRecord(JsonNode k, JsonNode v) {
		
		System.out.println("in filter");
		if (v != null) {
			
			System.out.println("in filter: returning true");
			return true;
		}else {
			
			System.out.println("in filter: returning false");
			return false;
		}
		
		
		
	}
	
	
} // end-class