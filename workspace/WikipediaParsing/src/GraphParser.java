import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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

import types.LongArrayWritable;
import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to change a Wikipedia dumpset
 *  into a graph, based on the pages and their
 *  intermediate links. Requires a mapping
 *  from Title to Docid as input
 * @author stevenb
 * @date 08-08-2013
 */
public class GraphParser extends Configured implements Tool {
	
	public static final int INITIAL_CAPACITY = 17919895; //Initial-Capacity = (Number-of-Wikipedia-pages / 0.75) + 1
	public static final String MAPPINGFILE = "mapping"; //File the path should be equal to
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, Text, LongArrayWritable> {
		
		private HashMap<String, Long> title_docidMap = null;
		
		@Override
		public void setup(Context context) {
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (paths != null && paths.length > 0) {
					loadTitleDocidMapping(context, paths[0]);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private void loadTitleDocidMapping(Context context, Path path) {
			BufferedReader br = null;
			long counter = 0;
			
			try {
				br = new BufferedReader(new FileReader(path.toString()));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				System.out.println("read from distributed cache: file not found!");
			}
			
			try {
				String line = "";
				title_docidMap = new HashMap<String, Long>(INITIAL_CAPACITY);
				while ((line = br.readLine()) != null) {
					String[] arr = line.split("\t");
					if (arr.length == 2) {
						counter++;
						context.setStatus("Progressing:" + counter);
						context.progress();
						title_docidMap.put(arr[0], Long.parseLong(arr[1]));
					}
				}
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("read from distributed cache: read length and instances");
			}
			System.out.printf("LOAD_TITLE-DOCID_MAPPING done\n"); // TODO REMOVE
		}
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (title_docidMap == null || value.isEmpty()) {
				return;
			}
			Text docid = new Text(value.getDocid() + " 1");//'1' is the initial Pagerank mass
			List<String> allLinksList = value.extractLinkDestinations();
			Iterator<String> linkIterator = allLinksList.iterator();
			ArrayList<LongWritable> linksList = new ArrayList<LongWritable>();
			
			while (linkIterator.hasNext()) {
				String link = linkIterator.next().toLowerCase();//Should be lowercase to make sure a correct comparison is made | HashMap contains lowercase keys
				if (title_docidMap.containsKey(link)) {
					LongWritable linkDocid = new LongWritable(title_docidMap.get(link));
					if (!linksList.contains(linkDocid)) {
						linksList.add(linkDocid);
					}
				}
			}
			
			LongWritable[] linksArray = new LongWritable[linksList.size()];
			linksList.toArray(linksArray);
			LongArrayWritable links = new LongArrayWritable(LongWritable.class);
			links.set(linksArray);
			context.write(docid, links);
		}
	}
	
	public void createJob(Configuration conf, String inputString, String outputString) throws IOException, InterruptedException, ClassNotFoundException {
		conf.set("mapred.child.java.opts", "-Xmx4096m"); // Configuration settings
		conf.set("mapred.task.timeout", "0");
		conf.set("wiki.language", "en");
		
		Path filepath = new Path(conf.get(MAPPINGFILE)); // Distributed Cache settings
		FileSystem fs = FileSystem.get(filepath.toUri(), conf);
		filepath = fs.makeQualified(filepath);
		DistributedCache.addCacheFile(filepath.toUri(), conf);
		
		Job job = new Job(conf, "Graph Parser"); // Main settings
		job.setJarByClass(GraphParser.class);
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
		System.out.println("usage:\t <input path> <output path> <title-docid mapping path>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
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
		conf.set(MAPPINGFILE, args[2]);
		
		// Create and start iterations
		createJob(conf, inputString, outputString);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new GraphParser(), args);
		System.exit(result);
	}
}
