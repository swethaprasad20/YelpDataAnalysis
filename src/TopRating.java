import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopRating {

	private static final String OUTPUT_PATH = "/sxp142031/intermediate_output";
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\\^");
			word.set(mydata[2]);
			context.write(word, new FloatWritable(Float.parseFloat(mydata[3])));
		}
	}



	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0; 
			int count=0;
			for (FloatWritable val : values) {
				count++;
				sum += val.get();
			}
			double average = (double)sum/count;
			result.set((float) average);
			context.write(key, result);


		}


	}
	public static class Map2 extends Mapper<LongWritable, Text, FloatWritable, Text>{
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\s+");
			word.set(mydata[0]);
			context.write(new FloatWritable(Float.parseFloat(mydata[1])),word);
		}
	}

	public static class Reduce2 extends Reducer<FloatWritable,Text,Text,NullWritable> {
		int count=0;

		public void reduce(FloatWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				if(count>=10)
					break;
				count++;
				context.write(val, null);
			}
		}
	}

	public static class DescendingOrderComparator extends WritableComparator {
		public DescendingOrderComparator() {
			super(FloatWritable.class, true);
		}
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2) {
			FloatWritable value1 = (FloatWritable) tp1;
			FloatWritable value2 = (FloatWritable) tp2;
			return -1 * value1.compareTo(value2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:TopRating  <in> <out>");

			System.exit(2);
		}

		Job job = new Job (conf, "Job 1");
		job.setJarByClass(TopRating.class); 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Job 2");
		job2.setJarByClass(TopRating.class);
		job2.setNumReduceTasks(1);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setSortComparatorClass(DescendingOrderComparator.class);
		job2.setMapOutputKeyClass(FloatWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);


		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));


		System.exit(job2.waitForCompletion(true) ? 0 : 1);



	}

}