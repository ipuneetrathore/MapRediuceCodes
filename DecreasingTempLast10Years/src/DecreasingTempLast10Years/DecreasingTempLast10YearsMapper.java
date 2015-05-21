// Adding package name (project name)
package DecreasingTempLast10Years;

// Importing required APIs
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Creating Mapper Class
public class DecreasingTempLast10YearsMapper 
extends Mapper<Object, Text, FloatWritable, Text>{

// Creating (key,value) pair (Key as Village and Value as the temperature) 
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String[] fileData = value.toString().split(",");
	String maxTemperature = fileData[9];
	String minTemperature = fileData[10];
	
	try
	{
		Float max = Float.parseFloat(maxTemperature);
		Float min = Float.parseFloat(minTemperature);
		context.write(new FloatWritable(max),value);
		context.write(new FloatWritable(min),value);
	}
	catch(NumberFormatException ex){}
	

}

}