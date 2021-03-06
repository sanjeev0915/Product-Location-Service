package jain.sanjeev.kafka;


import org.apache.kafka.streams.kstream.ValueTransformer;


import org.apache.kafka.streams.processor.ProcessorContext;

import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/*
 * This class will extends the ValueTransformer  to transform the input JSON Record (coming from Stream) to null if it is already been processed earlier
 * Transform method in this class will check the record's Key in a timed window cache of defined period (configurable). If Key is found in the cache this out record
 * is set to null and returned back. if key is not found in the cache it means the record came here first time, this record will be forwarded to the downstream processes
 * at it is and a entry will also be created in the cache. 
 * 
 * Kafka stream WindowStore is used as a cache
 */

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
		

	
		try {
			
			String recordValue = (String) value.toString();
		
			rootNode = 	om.readTree(recordValue);
			
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
				System.out.println("searching " + userName + " in last 10 minutes");
				
				WindowStoreIterator<String> iterator = windowStore.fetch(userName,from, to); // fetch all records from window store with key in time range from and to
				boolean entryFound = iterator.hasNext(); //  check if a record with exists in window store using iterator
				
				if(!entryFound) { // if entry  not found in the defined time window, add the entry in window store
					

					// get event-time from the record
					
					JsonNode datetimeNode = rootNode.path("MODIFIED");
					String recTime = (String)datetimeNode.asText();
					SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					Date d = f.parse(recTime);
					
					long datetime = d.getTime();
					
					
					windowStore.put(userName,userName,datetime);        // adding a record in the window store - key, Value, eventTime,
					
					System.out.println("in Transformer3: not in state store.ading it now: "+ userName + ", at " + new Date(datetime));
					
					return rootNode;
		
				}else { // if found in store, it means duplicate, so set the return record to null which will be filtered in next processing node. Kstream.filter
					
					
					System.out.println("in Transformer3: found in state store: " + userName);
				
					rootNode=null;
					return rootNode;
					
					
				}
		
			
			}else // if rootNode == Null
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


}
