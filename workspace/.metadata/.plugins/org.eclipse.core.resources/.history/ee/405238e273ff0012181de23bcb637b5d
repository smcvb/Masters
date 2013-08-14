import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikipediaLinkCounter {
	
	public static final int REDUCE_TASKS = 10;
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text link = new Text();
		private IntWritable counter = new IntWritable();
		private HashMap<String, Integer> linkMap = null;
		
		@Override
		public void setup(Context context) {
			link = new Text();
			counter = new IntWritable();
			linkMap = new HashMap<String, Integer>();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String linkString = "",
					line = value.toString();
			String[] links = line.split("\\s+");
			for(int i = 2; i < links.length; i++){
				linkString = links[i].toLowerCase();
				if (!linkString.equals("")) {
					linkMap.put(linkString, linkMap.containsKey(linkString) ? linkMap.get(linkString) + 1 : 1);
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, Integer> entry : linkMap.entrySet()) {
				link.set(entry.getKey());
				counter.set(entry.getValue());
				context.write(link, counter);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable reducedValue = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			reducedValue.set(sum);
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void printUsage(int argLength) {
		if (argLength < 2) {
			System.out.println("usage:\t <input path> <output path> <number of reduce tasks [default 10]>");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException, URISyntaxException {
		printUsage(args.length);
		conf.set("mapred.task.timeout", "0");
		conf.set("wiki.language", "en");
		
		Job job = new Job(conf, "Wikipedia Link Counter");
		job.setJarByClass(WikipediaLinkCounter.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); //Input settings
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class); //Class settings
		job.setReducerClass(Reduce.class);
		
		if(args.length > 2)
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		if(args.length == 2)
			job.setNumReduceTasks(REDUCE_TASKS);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = createJob(args, conf);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
	}
}
