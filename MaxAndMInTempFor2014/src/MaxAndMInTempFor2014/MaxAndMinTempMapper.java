package MaxAndMInTempFor2014;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class MaxAndMinTempMapper extends Mapper<Object, Text, Text, Text> {
	
	private Set<String> HashedDta = new HashSet<String>();
	
	// fileData[7]- village name, [8]- date,  [9] - MinTemp, [10]- MaxTemp
		@Override
	    public void map(Object key, Text value, Context output) throws IOException,
	            InterruptedException {
			String[] fileData = value.toString().split(",");
	        String keyTempData = fileData[7]+"#"+fileData[8]+"#"+fileData[9]
	        		+"#"+fileData[10];
	        String[] Year = fileData[8].split("-");
	        		
	        if (Year[1]=="2014"){
	        HashedDta.add(keyTempData);
	        }
	    }
		
		@Override
		protected void cleanup(
		Context context)
		throws IOException, InterruptedException {
			
			Text outputKeyMax = new Text();
			outputKeyMax.set("MaxTemperatureLocation");
			Text outputKeyMin = new Text();
			outputKeyMin.set("MinTemperatureLocation");
			
			
			Float max_temp = 0F;
    		Float min_temp = 0F;
    		
    		String max_temp_val = "";
    		String min_temp_val = "";
    		
	        
    		//split using comma.
    		Float curr_max_temp = 0F;
    		Float curr_min_temp = 0F;
    		
    		// fileData[7]- village name, [8]- date,  [9] - MinTemp, [10]- MaxTemp
    		for(String key: HashedDta) {	
			
				String[] fileData = key.toString().split("#");

				String keyTempData = fileData[0]+"#"+fileData[1]+"#"+fileData[2]+"#"+fileData[3];
			
				try
		        {
					curr_min_temp = Float.parseFloat(fileData[2]);
		        	curr_max_temp = Float.parseFloat(fileData[3]);

					if (max_temp == 0)
			        {
			        	max_temp = curr_max_temp;
			        	max_temp_val = keyTempData;
			        }
			        if (min_temp == 0)
			        {
			        	min_temp = curr_min_temp;
			        	min_temp_val = keyTempData;
			        }
		        	if (curr_max_temp > max_temp)
			        {
			        	max_temp = curr_max_temp;
			        	max_temp_val = keyTempData;
			        }
				
			        if (curr_min_temp < min_temp)
			        {
			        	min_temp = curr_min_temp;
			        	min_temp_val = keyTempData;
			        }
		        }
		        catch(NumberFormatException ex){}
		        
		    
		}
    		context.write(outputKeyMax, new Text(max_temp_val));
			context.write(outputKeyMin, new Text(min_temp_val));
		
		}
}
