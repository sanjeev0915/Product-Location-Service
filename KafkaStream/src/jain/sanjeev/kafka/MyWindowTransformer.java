package jain.sanjeev.kafka;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class MyWindowTransformer implements ValueTransformer<JsonNode, JsonNode>{
	


    private WindowStore<String, String> windowStore;
    private final String storeName;
    private ProcessorContext context;
    private static final Integer BACKINTIME = 10*60*1000;
	
    public MyWindowTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        windowStore = (WindowStore<String,String>) this.context.getStateStore(storeName);
    }
    
    @Override
    public JsonNode transform(JsonNode value) {
    	
		ObjectMapper om = new ObjectMapper();
		JsonNode rootNode = null;
		String mapData = null;
		
	
	
		System.out.println("in Transformer1");
	
		try {
			
			 mapData = (String) value.toString();
		
			rootNode = 	om.readTree(mapData);
			
			if (!rootNode.isNull()) {
				System.out.println("in Transformer2: not null");
						
				// get userName from the record
				
				JsonNode userNode = rootNode.path("USERNAME");
				String userName = (String)userNode.asText();

	
				
				/****                     Search in window store  in last one minute    *****/
			 		
				long currentTime = context.timestamp();
				long from = currentTime - BACKINTIME/2; // setting the window segment starting time
				long to = currentTime + BACKINTIME/2;	 // setting the window segment end time
				
				long diff = to-from;
				System.out.println(new Date(from) + ", "+ new Date(to) + ", "  +diff);

				
				boolean entryFound=false;
				
		
				
				
				KeyValueIterator iter = windowStore.all();
			
				while(iter.hasNext()) {
					KeyValue kv = (KeyValue)iter.next();
					
					Windowed kw = (Windowed)kv.key;
					System.out.println(kw.key());
				//	System.out.println(kw.window().toString());
					System.out.println("from: " + new Date(kw.window().start()) + ", to: " + new Date(kw.window().end()));
					
					
					System.out.println(kv.key.toString() +", " + kv.value.toString());
					
				}
				
				
				
				WindowStoreIterator<String> iterator = windowStore.fetch(userName,from, to); // fetch with key in range from and to
				System.out.println("searching " + userName + " in last 10 minutes");

				
				entryFound = iterator.hasNext();
				
		/*		while (iterator.hasNext()) {
					System.out.println("found something in last 10 minute");
					
					KeyValue<Long, String> rec =  iterator.next(); // KeyValue is pair of (long, Value) where long is timestamp
					
					String stateStoreValue = (String)rec.value;
					System.out.println(rec.key + ", "+rec.value);
					if (stateStoreValue.equals(userName)) {
						entryFound=true;
						iterator.close();
						break;
					}
				}*/
				
				/****                     Search in window store  in last one minute    *****/

				
				if(!entryFound) { // not found in the defined time window
					

					// get event-time from the record
					
					JsonNode datetimeNode = rootNode.path("MODIFIED");
					String recTime = (String)datetimeNode.asText();
					SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					Date d = f.parse(recTime);
					
					long datetime = d.getTime();
					
					// adding a record in the window store - key, Value, eventTime,
							windowStore.put(userName,userName,datetime);
					
					System.out.println("in Transformer3: not in state store.ading it now: "+ userName + ", at " + new Date(datetime));
					
					return rootNode;
		
				}else { // if found in store
					
					
					System.out.println("in Transformer3: found in state store: " + userName);
				
					rootNode=null;
					return rootNode;
					
					
				}
		
			
			}else
			{
				System.out.println("in Transformer2: null");
			}
		
		} catch(Exception e) {
			e.printStackTrace();
		}
		

return rootNode;
       

    }
    
    @Override
    @SuppressWarnings("deprecation")
    public JsonNode punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {
        //no-op
    }

//    static long getCurrentTime() {
//    	
//    	long currentTime;
//    	Instant instant = Instant.now();
//    	currentTime = instant.toEpochMilli();
//    	
//    	return currentTime;
//    }
}
