import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemp{

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		Text k = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			while(tokenizer.hasMoreTokens())
			{
				String year = tokenizer.nextToken();
				String temp = tokenizer.nextToken().trim();

				k.set(year);
				int val = Integer.parseInt(temp);
				context.write(k, new IntWritable(v));
			}
		}
	}


	public static class Reduce extends Reduced<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context){
			int maxTemp = -9999;
			for(IntWritable x: values){
				if(maxTemp < x.get())
					maxTemp = x;
			}
			context.write(key, new IntWritable(maxTemp));
		}
	}

	public static void main(String[] args){
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Max Temperature");

		job.setJar("maxtemp.jar");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}