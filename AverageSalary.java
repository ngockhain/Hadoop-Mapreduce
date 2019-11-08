import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalary{
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		private Text id = new Text();
		private DoubleWritable salary = new DoubleWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String val[] = line.split(" ");
			id.set(val[0]);
			salary.set(Double.parseDouble(val[2]));

			context.write(id, salary);
		}
	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double cnt = 0, sum = 0;
			for(DoubleWritable x: values)
			{
				sum += x.get();
				cnt+=1;
			}
			context.write(key, new DoubleWritable(sum/cnt));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageSalary");

		job.setJar("averagesalary.jar");

		job.setJarByClass(AverageSalary.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(FileInputFormat.class);
		job.setOutputFormatClass(FileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}