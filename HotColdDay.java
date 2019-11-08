import java.io.IOException;
import java.util.StringTokenizer; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class HotColdDay {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override 
        public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
        	
        	String line = value.toString();
        	String[] lineInputs = line.split("\\s+");
        	
        	Text date = new Text();
        	date.set(lineInputs[1]);
        	
        	Double maxTemp = Double.parseDouble(lineInputs[4]);
        	Double minTemp = Double.parseDouble(lineInputs[5]);
        	
        	String day = "";
        	
        	if (minTemp < 10 & maxTemp > 40)
        	{
        		day = "Hot and Cold Day";
        	}
        	else if(minTemp < 10 ) {
        		day = "Cold Day";
        	}
        	else if (maxTemp > 40)
        		day = "Hot Day";
        	if (day != ""){
        		Text dayText = new Text();
        		dayText.set(day);      	
            	context.write(date, dayText);
 			}
        } 
    } 
    
      	
    
    public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "HotColdDay");
		job.setJar("hotcold.jar");
		job.setJarByClass(HotColdDay.class);
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		
       	Path outputPath = new Path(args[1]);
        
    	FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
 
    }
}
