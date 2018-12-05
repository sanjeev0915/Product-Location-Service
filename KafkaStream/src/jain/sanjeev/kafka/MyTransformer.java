package jain.sanjeev.kafka;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.Objects;

public class MyTransformer implements ValueTransformer<JsonNode, JsonNode>{
	

    private KeyValueStore<String, String> stateStore;

    private final String storeName;
    private ProcessorContext context;
	
    public MyTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<String, String>) this.context.getStateStore(storeName);
 

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
						
				JsonNode userNode = rootNode.path("USERNAME");
			
				StringBuilder sb = new StringBuilder((String)userNode.asText());
				
				String valueInStore = stateStore.get(sb.toString());
				
				if(valueInStore == null) { // notfound
					stateStore.put(sb.toString(),sb.toString());
					
					System.out.println("in Transformer3: not in state store.ading it now: "+ valueInStore);
					
					return rootNode;
		
				}else { // if found in store
					
					
					System.out.println("in Transformer3: found in state store: " + valueInStore);
					// value already exists, dupFlag=true
//					String s = sb.toString().toUpperCase();
//					char startChar = (s.toCharArray())[0];
//					if(startChar >=65 && startChar <= 71) 
//					{
//						System.out.println("in Transformer3: setting rootnode to null");
//						rootNode = null;
//					}
//					
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


}
