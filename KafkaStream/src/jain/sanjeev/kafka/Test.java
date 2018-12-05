package jain.sanjeev.kafka;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.fasterxml.jackson.databind.JsonNode;

public class Test {

	public static void main(String...args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "My_First");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
	        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,  "org.apache.kafka.connect.json.JsonDeserializer");
		
		StreamsConfig sg = new StreamsConfig(props);
		
		
				
				
				JsonSerializer js = new	JsonSerializer();
				JsonDeserializer jds = new JsonDeserializer();
				
				Serde<JsonNode> jsonSerde = Serdes.serdeFrom(js,jds);
				
				
		StreamsBuilder sb = new StreamsBuilder();
		
		KStream <JsonNode, JsonNode> kStream  = sb.stream("connet-test", Consumed.with(jsonSerde,jsonSerde));
		
		
		
		
	}
	
	
}
