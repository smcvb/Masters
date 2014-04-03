import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to create a mapping
 *  from title to docid from a
 *  Wikipedia dump file.
 * Will ignore empty and redirect 
 *  pages, since the first doesn't
 *  point to anything and the latter
 *  is a loop.
 * @author stevenb
 * @date 08-08-2013
 */
public class TitleDocidParser extends Configured implements Tool {
	
	public static enum Counters {
		DOUBLE_ID
	}
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isEmpty() || value.isRedirect()) {
				return;
			}
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
			while (valueIterator.hasNext()) {
				counter++;
				context.write(key, valueIterator.next());
			}
			if (counter > 1) {
				context.getCounter(Counters.DOUBLE_ID).increment(1);
				System.out.printf("Key %s occured multiple (%d) times\n", key.toString(), counter);
			}
		}
	}
	
	public void createJob(Configuration conf, String inputString, String outputString) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("wiki.language", "en");
		Job job = new Job(conf, "Title/Docid Parser");
		job.setJarByClass(TitleDocidParser.class);
		FileInputFormat.setInputPaths(job, new Path(inputString)); //Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputString)); //Output settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Map.class); //Class settings
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
	}
	
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> ");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputString = "", outputString = "";
		Configuration conf = new Configuration(getConf());
		
		// Set arguments
		if (args.length < 2) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputString = args[0];
		outputString = args[1];
		
		// Create and start iterations
		createJob(conf, inputString, outputString);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TitleDocidParser(), args);
		System.exit(result);
	}
}
