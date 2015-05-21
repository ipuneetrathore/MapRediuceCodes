package DecreasingTempLast10Years;

// importing the APIs
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Default code for all the drivers
public class DecreasingTempDriver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DecreasingTempDriver(), args);
        System.exit(res);       
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DecreasingTempLast10YearsMapper.class);
        job.setReducerClass(DecreasingTempLast10YearsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setSortComparatorClass(ReverseDataGenerator.class);
        

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(DecreasingTempDriver.class);
        
        job.submit();
        return 0;
    }
    
    
}