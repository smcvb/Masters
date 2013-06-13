import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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

import types.LongArrayWritable;
import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

public class WikipediaGraphParser {
	
	public static final long INITIAL_CAPACITY = 17946561; //Initial-Capacity = (Number-of-Wikipedia-pages / 0.75) + 1
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, Text, LongArrayWritable> {
		
		private HashMap<String, Long> title_docidMap;
		
		@Override
		public void setup(Context context) {
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (paths != null && paths.length > 0) {
					for (Path path : paths) {
						if (path.getName().equals("mappingLowercase")) {
							loadTitleDocidMapping(context, path);
							break;
						}
					}
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
				title_docidMap = new HashMap<String, Long>(17946561);
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
			System.out.printf("Pages: " + counter + "\n"); //TODO remove
		}
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isEmpty()) {
				return;
			}
			Text docid = new Text(value.getDocid() + " 1");
			int mappingsfound = 0;
			List<String> allLinksList = value.extractLinkDestinations();
			Iterator<String> linkIterator = allLinksList.iterator();
			//LongWritable[] allLinks = new LongWritable[linksList.size()];//TODO remove
			ArrayList<LongWritable> linksList = new ArrayList<LongWritable>();
			//ArrayList<String> doubleLinksList = new ArrayList<String>();//TODO remove
			
			int location = 0;//TODO remove
			System.out.printf("TITLE: %s Actual output: %s \n", value.getTitle(), docid.toString());//TODO remove
			while (linkIterator.hasNext()) {
				String link = linkIterator.next().toLowerCase();//Should be lowercase to make sure a correct comparison is made | HashMap contains lowercase keys
				System.out.printf("link:%s ", link);//TODO remove
				if (title_docidMap.containsKey(link)) {
					LongWritable linkDocid = new LongWritable(title_docidMap.get(link));
					if (!linksList.contains(linkDocid.get())) {
						linksList.add(linkDocid);
						mappingsfound++;//TODO remove???
						System.out.printf("ADDED docid:%d \n", linksList.get(linksList.size() - 1).get());//TODO remove
					}
				}
				else {//TODO remove
					System.out.printf("LEFT OUT");//TODO remove
				}//TODO remove
				location++;//TODO remove
			}
			System.out.printf("\nLINKS | Locations: %d Mappings: %d\n", location, mappingsfound);//TODO remove
			
			LongWritable[] linksArray = new LongWritable[linksList.size()];
			linksList.toArray(linksArray);
			LongArrayWritable links = new LongArrayWritable(LongWritable.class);
			links.set(linksArray);
			context.write(docid, links);
		}
	}
	
	public static void printUsage(int argLength) {
		if (argLength < 3) {
			System.out.println("usage:\t <input path> <output path> <title-docid mapping path>");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException, URISyntaxException {
		printUsage(args.length);
		conf.set("mapping.path", args[2]);
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.set("mapred.task.timeout", "0");
		conf.set("wiki.language", "en");
		
		Path filepath = new Path(args[2]);
		FileSystem fs = FileSystem.get(filepath.toUri(), conf);
		filepath = fs.makeQualified(filepath);
		DistributedCache.addCacheFile(filepath.toUri(), conf);
		
		Job job = new Job(conf, "Wikipedia Graph Parser");
		job.setJarByClass(WikipediaGraphParser.class);
		FileInputFormat.setInputPaths(job, new Path(args[0])); //Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongArrayWritable.class);
		job.setMapperClass(Map.class); //Class settings
		job.setNumReduceTasks(0);//If set to zero, Reducer is ignored and Mapper will be the output
		
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
