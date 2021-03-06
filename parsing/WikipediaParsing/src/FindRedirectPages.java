import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to find all 
 *  redirect pages in a Wikipedia
 *  dump file
 * @author stevenb
 * @date 08-08-2013
 */
public class FindRedirectPages extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isRedirect()) {
				context.write(new LongWritable(Long.parseLong(value.getDocid())), new Text(String.format("TITLE:%s\tFILE:\n%s", value.getTitle(), value.getContent())));
			}
		}
	}
	
	public void createJob(Configuration conf, String inputString, String outputString) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("wiki.language", "en");
		Job job = new Job(conf, "Find Redirect Wikipediapages"); // Main settings
		job.setJarByClass(FindRedirectPages.class);
		FileInputFormat.setInputPaths(job, new Path(inputString)); // Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputString)); // Output settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class); // Class settings
		job.setNumReduceTasks(0);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
	}
	
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path>");
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
		int result = ToolRunner.run(new Configuration(), new FindRedirectPages(), args);
		System.exit(result);
	}
}
