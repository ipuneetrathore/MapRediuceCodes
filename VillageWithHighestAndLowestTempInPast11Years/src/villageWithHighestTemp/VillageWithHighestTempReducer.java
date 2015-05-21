package villageWithHighestTemp;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class VillageWithHighestTempReducer extends Reducer<Text, Text, Text, Text> {

	@Override
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
		Text outputKeySort = new Text();
		outputKeySort.set("MaxTemperatureDesc");
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
		
		
		for(Text value: values) {	
			if(value.toString() == null || value.toString().isEmpty() )
			{
				continue;
			}
			String[] fileData = value.toString().split("#");
		//	if(fileData.length != 3)
		//	{
		//		continue;
		//	}
			String keyTempData = fileData[0]+"#"+fileData[1]+"#"+fileData[2]+"#"+fileData[3];
			
			try
	        {
	        	if (key.equals(outputKeyMax))
	        	{
		        	curr_max_temp = Float.parseFloat(fileData[3]);
		        	if (max_temp == 0)
			        {
			        	max_temp = curr_max_temp;
			        	max_temp_val = keyTempData;
			        }
		        	if (curr_max_temp > max_temp)
			        {
			        	max_temp = curr_max_temp;
			        	max_temp_val = keyTempData;
			        }
	        	}
	        	if (key.equals(outputKeyMin))
	        	{
		        	curr_min_temp = Float.parseFloat(fileData[2]);
		        	if (min_temp == 0)
			        {
			        	min_temp = curr_min_temp;
			        	min_temp_val = keyTempData;
			        }
			        if (curr_min_temp < min_temp)
			        {
			        	min_temp = curr_min_temp;
			        	min_temp_val = keyTempData;
			        }
	        	}
	        }
	        catch(NumberFormatException ex){}
	        
	
    }

		if (key.equals(outputKeyMax) && max_temp_val.length() != 0)
    	{
			String[] temp = max_temp_val.split("#");
			String out_data = "The maximum temperature for 11 years in "+temp[0]+"for the year "+temp[1]+" is "+temp[3];
			output.write(outputKeyMax, new Text(out_data));
    	}
		if (key.equals(outputKeyMin) && min_temp_val.length() != 0)
    	{
			String[] temp = min_temp_val.split("#");
			String out_data = "The minimum temperature for 11 years in "+temp[0]+"for the year "+temp[1]+" is "+temp[2];
			output.write(outputKeyMin, new Text(out_data));
    	}
		
	}
}