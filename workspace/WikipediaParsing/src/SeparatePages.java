import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.TextLongPair;
import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to separate all
 *  the wikipedia pages from a 
 *  dumb file as loss <text-docid, null>
 *  sequence files.
 * @author stevenb
 * @date 17-10-2013
 */
public class SeparatePages extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, TextLongPair, IntWritable> {
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isEmpty()) { // An empty WikipediaPage was encountered, hence nothing to retrieve terms from
				return;
			}
			Text x = new Text(String.format("\tisArticle:%b\n" +
					"\n\tisRedirect:%b\n" +
					"\n\tisDisambiguation:%b\n" +
					"\n\tisStub:%b\n" +
					"\n\tisEmpty:%b\n", 
					value.isArticle(), value.isRedirect(), value.isDisambiguation(), value.isStub(), value.isEmpty()));
			System.out.println(x.toString());
			
			String text = value.getContent();
			long docid = Long.parseLong(value.getDocid());
			TextLongPair output = new TextLongPair(text, docid);
			context.write(output, new IntWritable(0));
		}
	}
	
	public static class Reduce extends Reducer<TextLongPair, IntWritable, TextLongPair, IntWritable> {
		public void reduce(TextLongPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, new IntWritable(0));
		}
	}
	
	public Job createJob(Configuration conf, String inputString, String outputString, int reduceTasks) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("wiki.language", "en");
		Job job = new Job(conf, "Separate Wikipedia Pages"); // Main settings
		job.setJarByClass(SeparatePages.class);
		FileInputFormat.setInputPaths(job, new Path(inputString)); // Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputString)); // Output settings
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(TextLongPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class); // Class settings
		job.setNumReduceTasks(reduceTasks);
		if(reduceTasks > 0){
			job.setReducerClass(Reduce.class);
		}
		
		return job;
	}
	
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> [reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int reduceTasks = 0; //If no [reducers] parameter is given, the number of output partitions will equal the number of input partitions.
		String inputString = "", outputString = "";
		Configuration conf = new Configuration();
		
		// Set arguments
		if (args.length < 2) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputString = args[0];
		outputString = args[1];
		if(args.length >= 3){
			try {
				reduceTasks = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Error: expected Integer instead of " + args[2]);
				return printUsage();
			}
		}
		
		// Create and start iterations
		Job job = createJob(conf, inputString, outputString, reduceTasks);
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new SeparatePages(), args);
		System.exit(result);
	}
}
