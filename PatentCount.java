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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PatentCount{
	public static class Map extends Mapper <LongWritable, Text, Text, Text>{
		Text keys = new Text();
		Text values = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line, " ");

			while(tokenizer.hasMoreTokens())
			{
				String k = tokenizer.nextToken();
				String v = tokenizer.nextToken();
				keys.set(k);
				values.set(v);

				context.write(keys, values);
			}
		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int cnt = 0;
			for (Text x: values){
				cnt += 1;
			}
			context.write(key, new IntWritable(cnt));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Patent Count");

		job.setJar("patentcount.jar");
		job.setJarByClass(PatentCount.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
