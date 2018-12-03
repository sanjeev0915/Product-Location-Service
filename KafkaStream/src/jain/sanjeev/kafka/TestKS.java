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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import com.fasterxml.jackson.databind.JsonNode;




public class TestKS {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MyKafkaStreaTest ");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class);
		
	    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
	    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
	    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
	    
	    String winDupStateStoreName = "WinDupStore";
		
		StreamsConfig streamingConfig = new StreamsConfig(props);
		System.out.println("step1");
		
		StreamsBuilder builder = new StreamsBuilder();
		/**** Creating a Window  State Store  of winodow size 1 minute (600000 milli seconds) and 5 minutes of retention ***********/
		
		/*
		 * store name
		 * retention period  (in millisecs)
		 * num of segments
		 * window Size (in millisecs)
		 * retain duplicates
		 */
		
		WindowBytesStoreSupplier windowStoreSupplier = Stores.persistentWindowStore(winDupStateStoreName, 600000, 2, 600000, false);
		
		StoreBuilder<WindowStore<String,String>> windowStoreBuilder = Stores.windowStoreBuilder(windowStoreSupplier, Serdes.String(), Serdes.String());
		
		builder.addStateStore(windowStoreBuilder);
		
		System.out.println("step2");
		
		/**** Finished Creating State Store  ***********/
		
		
		// this will read the messages from Kafka Topic connect-test
		KStream<JsonNode, JsonNode> simpleFirstStream = 
				builder.stream("connect-test",Consumed.with(jsonSerde, jsonSerde));
	
		System.out.println("step3");
		
		// Transforms the message to different message with updated values
	
		KStream<JsonNode, JsonNode> updateStream = simpleFirstStream
				.transformValues(()-> new MyWindowTransformer(winDupStateStoreName),winDupStateStoreName)
				.filter(TestKafkaStream::filterRecord);
		
		System.out.println("step4");
		
		// writing to output - topic
		
		updateStream.to( "testout",Produced.with(jsonSerde, jsonSerde));
		System.out.println("step5");
		
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamingConfig);
		System.out.println("step6");
		

		// Starting the stream
		
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