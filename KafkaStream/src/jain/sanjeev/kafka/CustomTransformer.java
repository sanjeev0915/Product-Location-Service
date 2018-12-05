package jain.sanjeev.kafka;
import org.apache.kafka.streams.kstream.Transformer;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.Objects;

public class CustomTransformer implements Transformer<JsonNode, JsonNode, Boolean>{
	

    private KeyValueStore<String, String> stateStore;
    private final String storeName;
    private ProcessorContext context;
	
    public CustomTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }
    
    @Override
    public Boolean transform(JsonNode value, JsonNode Value) {
    	
		ObjectMapper om = new ObjectMapper();
		JsonNode rootNode = null;
		String mapData = null;
		boolean dupFlag=false;
	
		try {
			
			 mapData = (String) value.toString();
		
			rootNode = 	om.readTree(mapData);
			
			if (!rootNode.isNull()) {
						
				JsonNode userNode = rootNode.path("USERNAME");
			
				StringBuilder sb = new StringBuilder((String)userNode.asText());
				if(stateStore.get(sb.toString()) == null) {
					stateStore.put(sb.toString(),sb.toString());
					dupFlag=false;
		
				}else {
					// value already exists, dupFlag=true
					dupFlag=true;
					
					
				}
		
			
			}
		
		} catch(Exception e) {
			e.printStackTrace();
		}
		


        return dupFlag;

    }
    
    @Override
    @SuppressWarnings("deprecation")
    public Boolean punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {
        //no-op
    }

}
