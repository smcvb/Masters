import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

public class WikipediaTitleDocidParser {
	public static class Map extends Mapper<LongWritable, WikipediaPage, Text, LongWritable> {
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isEmpty())
				return;
			LongWritable docid = new LongWritable(Long.parseLong(value.getDocid()));
			Text title = new Text(value.getTitle().toLowerCase());
			context.write(title, docid);
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> valueIterator = value.iterator();
			int counter = 0;
			while(valueIterator.hasNext()) {
				counter++;
				context.write(key, valueIterator.next());
			}
			if(counter > 1){
				System.out.printf("Key %s occured multiple (%d) times", key.toString(), counter);
			}
		}
	}
	
	public static void printUsage(int argLength) {
		if (argLength < 2) {
			System.out.println("usage:\t <input path> <output path> ");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException {
		printUsage(args.length);
		conf.set("wiki.language", "en");
		
		Job job = new Job(conf, "Wikipedia Title/Docid Parser");
		job.setJarByClass(WikipediaTitleDocidParser.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); //Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Map.class); //Class settings
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = createJob(args, conf);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}
