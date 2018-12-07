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



/*
 * This Program is the main Kafka Stream Program, which takes input records from a Kafka Topic then for each record it calls
 * -- a custom Transformer (called using KStream.transformValues() --> This will set the input record to null if it is a duplicate and pass it to filter process
 * -- a filter (called using KStream.Filter()) --> This filter the records whcih are set to null from the output of transformValues
 * -- write to output topic
 * 
 * In the beginning as a setup - this program also adds a Window State Store to the Kafka Stream Topology which will be accessed in the transformValues function
 * Window Size in this sample is 10 minutes and same is the retention period
 */
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
		// Transforms the message to different message with updated values
		KStream<JsonNode, JsonNode> simpleFirstStream = 
				builder.stream("connect-test",Consumed.with(jsonSerde, jsonSerde))
				.transformValues(()-> new MyWindowTransformer(winDupStateStoreName),winDupStateStoreName) // Transformer is called in this line using the class MyWindowTransformer
				.filter(TestKafkaStream::filterRecord);// Records are filtered in this line
	
		
/*		// Transforms the message to different message with updated values
	
		KStream<JsonNode, JsonNode> updateStream = simpleFirstStream
				.transformValues(()-> new MyWindowTransformer(winDupStateStoreName),winDupStateStoreName) // Transformer is called in this line using the class MyWindowTransformer
				.filter(TestKafkaStream::filterRecord); // Records are filtered in this line
		*/
		
		// writing to output - topic
		
		simpleFirstStream.to( "testout",Produced.with(jsonSerde, jsonSerde, new MyPartitioner())); // Writing to the output Topic - only filtered records
		
		
		
		
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


	/*
	 * Filter Predicate/rule to call Kstream.filter()
	 * This Rule will return the value true if input record is not null else return will false
	 * So in a sense - filter function of Kstream filter out the null records and forwards non-null to downstream process
	 */
	
	static boolean filterRecord(JsonNode k, JsonNode v) {
		

		if (v != null) {
			
			System.out.println("in filter: returning true");
			return true;
		}else {
			
			System.out.println("in filter: returning false");
			return false;
		}
		
		
		
	}
	
	
} // end-class