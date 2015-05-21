// Adding the package
package DecreasingTempLast10Years;

// Importing the required API
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DecreasingTempLast10YearsReducer  extends Reducer<FloatWritable, Text, FloatWritable, Text>{

//Catching (Key,value) pair from mapper and using context to emit the final output)
public void reduce(FloatWritable key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
	
	// For every value in values, emitting key value pair
	for(Text value : values){
		context.write(key, value);
	}
}

}