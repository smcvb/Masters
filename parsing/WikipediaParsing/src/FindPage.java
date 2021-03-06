import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to find a specific
 *  page in a Wikipedia dump based on
 *  its Docid
 * @author stevenb
 * @date 08-08-2013
 */
public class FindPage extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, LongWritable, Text> {
		
		private HashSet<Long> pagesToLookFor;
		
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			Text line = new Text();
			pagesToLookFor = new HashSet<Long>();
			
			FileSystem fs;
			try {
				fs = FileSystem.get(conf);
				LineReader reader = new LineReader(fs.open(new Path(conf.get("searchingFor"))));
				while (reader.readLine(line) != 0) {
					long docID = Long.parseLong(line.toString());
					System.out.printf("SETUP: Found docID %d\n", docID);
					pagesToLookFor.add(docID);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			long docID = Long.parseLong(value.getDocid());
			if (pagesToLookFor.contains(docID)) {
				System.out.printf("MAP: Found page with docID %d\n", docID);
				Text output = new Text(String.format("%s\n\tisArticle:%b\n" +
						"\n\tisRedirect:%b\n" +
						"\n\tisDisambiguation:%b\n" +
						"\n\tisStub:%b\n" +
						"\n\tisEmpty:%b\n", 
						value.getContent(), value.isArticle(), value.isRedirect(), value.isDisambiguation(), value.isStub(), value.isEmpty()));
				context.write(new LongWritable(docID), output);
			}
		}
	}
	
	public void createJob(Configuration conf, String inputString, String outputString) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("wiki.language", "en");
		Job job = new Job(conf, "Find Wikipediapage"); // Main settings
		job.setJarByClass(FindPage.class);
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
		System.out.println("usage:\t <input path> <output path> <pages to find>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		String inputString = "", outputString = "";
		Configuration conf = new Configuration(getConf());
		
		// Set arguments
		if (args.length < 3) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputString = args[0];
		outputString = args[1];
		conf.set("searchingFor", args[2]);
		
		// Create and start iterations
		createJob(conf, inputString, outputString);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FindPage(), args);
		System.exit(result);
	}
}
