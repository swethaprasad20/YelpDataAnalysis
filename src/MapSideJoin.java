import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoin {
	private static final String OUTPUT_PATH = "/sxp142031/intermediate_output";
	public static class Map1 extends Mapper<LongWritable, Text, Text, NullWritable>{
		private Text word = new Text(); 
		public void map(LongWritable key, Text value, Context context

				) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\\^");
			if(mydata[1].contains("Stanford")){
				word.set(mydata[0]);
				context.write(word,null);
			}


		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
		private Map<String,String> businessIdMap=null;
		private Text userID = new Text();
		private Text rating = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			 businessIdMap = new HashMap<String,String>();
			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("Business");
			
			Path part=new Path("hdfs://cshadoop1"+myfilepath);//Location of file in HDFS


			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line=br.readLine();
				while (line != null){
					businessIdMap.put(line,line);
					line=br.readLine();
				}

			}
		}

		 
		public void map(LongWritable key, Text value, Context context

				) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\\^");
			if(businessIdMap.containsKey(mydata[2])){
				userID.set(mydata[1]);
				rating.set(mydata[3]);
				context.write(userID,rating);
			}


		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("MapSideJoin  <in> <in> <out>");

			System.exit(2);
		}

		Job job = new Job (conf, "MapSideJoin");
		job.setJarByClass(MapSideJoin.class); 
		job.setMapperClass(Map1.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();	
		conf2.set("Business", OUTPUT_PATH+File.separator+"part-m-00000");

		Job job2 = new Job (conf2, "MapSideJoin");

		job2.setJarByClass(MapSideJoin.class); 
		
		job2.setMapperClass(Map2.class);

		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Text.class);

		// set output value type 

		job2.setOutputValueClass(Text.class);

		//set the HDFS path of the input data

		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));

		// set the HDFS path for the output

		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

		//Wait till job completion

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}