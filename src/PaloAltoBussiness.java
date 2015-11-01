import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PaloAltoBussiness {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{


		private Text word = new Text(); // type of output key
		private Text busId = new Text("busId");
		public void map(LongWritable key, Text value, Context context

				) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\\^");
			if(mydata[1].contains("Palo Alto")){
				word.set(mydata[0]);
				context.write(busId,word);
			}


		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text busId = new Text("");

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				context.write(busId, val);
			}


		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:PaloAltoBussiness  <in> <out>");

			System.exit(2);
		}

		Job job = new Job (conf, "paloAlto");

		job.setJarByClass(PaloAltoBussiness.class); 

		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);

		// set output value type 

		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// set the HDFS path for the output

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Wait till job completion

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}