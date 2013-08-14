import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextLongPair;
import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

/**
 * Hadoop program to run the Inverted Indexing algorithm as
 *  specified in 'Data-Intensive Text Processing with MapReduce'
 * @author stevenb
 * @date 18-07-2013
 */
public class InvertedIndex extends Configured implements Tool {
	
	public static final int REDUCE_TASKS = 37; // 26 letters + 10 digits + 1 signs
	public static final String[] STOPWORDS = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
			"be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
			"can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
			"each",
			"few", "for", "from", "further",
			"had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
			"i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
			"let's",
			"me", "more", "most", "mustn't", "my", "myself",
			"no", "nor", "not",
			"of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
			"same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
			"than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
			"under", "until", "up",
			"very",
			"was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
			"you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
	}; // A list of stop words which are not important to map
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, TextLongPair, IntWritable> {
		
		private long currentDocid, previousDocid;
		private HashMap<String, Integer> postingTupleMap;
		public HashSet<String> stopwordSet;
		
		@Override
		public void setup(Context context) {
			currentDocid = 0;
			previousDocid = 0;
			postingTupleMap = new HashMap<String, Integer>();
			stopwordSet = new HashSet<String>();
			for (String stopword : STOPWORDS) {
				stopwordSet.add(stopword);
			}
		}
		
		@Override
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if (value.isEmpty()) { // An empty WikipediaPage was encountered, hence nothing to retrieve terms from
				return;
			}
			String term = "", contents = value.getContent(); // List containing terms to count frequency over
			currentDocid = Long.parseLong(value.getDocid());
			String[] terms = contents.split("\\s+");
			for (int i = 0; i < terms.length; i++) { // Pay load part of the inverted indexing algorithm | term frequency in this case
				term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
				if (!term.equals("") && !stopwordSet.contains(term)) {
					postingTupleMap.put(term, postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1);
				}
			}
			if (currentDocid != previousDocid) { //True: Finished a page, send to reducers
				for (Entry<String, Integer> entry : postingTupleMap.entrySet()) {
					TextLongPair tuple = new TextLongPair(entry.getKey(), currentDocid);
					IntWritable frequency = new IntWritable(entry.getValue());
					context.write(tuple, frequency);
				}
				postingTupleMap.clear(); // Empty memory
			}
			previousDocid = currentDocid; // Store previous found DocID because: 1. We finished a document and move to the next 2. We didn't finish, but reassign it
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException { // Clean last file
			for (Entry<String, Integer> entry : postingTupleMap.entrySet()) {
				TextLongPair tuple = new TextLongPair(entry.getKey(), previousDocid);
				IntWritable frequency = new IntWritable(entry.getValue());
				context.write(tuple, frequency);
			}
			postingTupleMap.clear(); // Empty memory
		}
	}
	
	public static class SortComparator extends WritableComparator {
		
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		private static final LongWritable.Comparator LONG_COMPARATOR = new LongWritable.Comparator();
		
		protected SortComparator() {
			super(TextLongPair.class, true);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return LONG_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextLongPair tp1 = (TextLongPair) w1;
			TextLongPair tp2 = (TextLongPair) w2;
			int cmp = tp1.getTerm().compareTo(tp2.getTerm());
			if (cmp != 0) {
				return cmp;
			}
			return tp1.getDocid().compareTo(tp2.getDocid());
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		
		private static final TextLongPair.Comparator TEXTLONGPAIR_COMPARATOR = new TextLongPair.Comparator();
		
		protected GroupComparator() {
			super(TextLongPair.class, true);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return TEXTLONGPAIR_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextLongPair tp1 = (TextLongPair) w1;
			TextLongPair tp2 = (TextLongPair) w2;
			return tp1.compareTo(tp2);
		}
	}
	
	public static class Partition extends Partitioner<TextLongPair, IntWritable> {
		
		@Override
		public int getPartition(TextLongPair tuple, IntWritable count, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			return Math.abs(tuple.getTerm().hashCode()) % numPartitions;
		}
	}
	
	public static class Reduce extends Reducer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable> {
		
		private String currentTerm, previousTerm;
		private ArrayList<TextIntPair> postingsList;
		private TextIntPairArrayWritable writablePostings;
		
		@Override
		public void setup(Context context) {
			currentTerm = "";
			previousTerm = null;
			postingsList = new ArrayList<TextIntPair>();
			writablePostings = new TextIntPairArrayWritable(TextIntPair.class);
		}
		
		@Override
		public void reduce(TextLongPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			currentTerm = key.getTerm().toString();
			if (!currentTerm.equals(previousTerm) && previousTerm != null) { // New term start, write out old term
				TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
				postingsArray = postingsList.toArray(postingsArray);
				writablePostings.set(postingsArray);
				context.write(new Text(previousTerm), writablePostings);
				postingsList.clear(); // Empty memory
			}
			// Write new docID and term frequency to this term
			TextIntPair posting = new TextIntPair(new Text(key.getDocid().toString()), new IntWritable(values.iterator().next().get()));
			postingsList.add(posting);
			previousTerm = currentTerm;
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException { 
			TextIntPair[] postingsArray = new TextIntPair[postingsList.size()]; // Clean last term
			postingsArray = postingsList.toArray(postingsArray);
			writablePostings.set(postingsArray);
			context.write(new Text(currentTerm), writablePostings);
			postingsList.clear(); // Empty memory
		}
	}
	
	/**
	 * Create the job.
	 * The conf.set()-s do the following:
	 * 	io.sort.mb: Give the sorting phase more memory
	 * 		Thus lowers the amount of read/writes to HDFS
	 *  mapred.child.java.opts: Gives the child's more memory.
	 *  	Default is 2048; doubled primarily for the reducers
	 *  mapred.task.timeout: When setting the timeout to 0, a task
	 *  	will never be killed if it reaches a certain no-response
	 *  	time
	 * 
	 * @param args: String array of arguments
	 * @param conf: a Configuration Object for the MapReduce job
	 * @return a finalized Job Object for this MapReduce job
	 * @throws IOException for creating the job and setting the input path
	 */
	private Job createJob(Configuration conf, String inputPath, String outputPath, int reduceTasks) throws IOException {
		conf.set("io.sort.mb", "512"); // Configuration settings
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.set("mapred.task.timeout", "0");
		conf.set("wiki.language", "en");
		
		Job job = new Job(conf, "Inverted Indexing"); // Main settings
		job.setJarByClass(InvertedIndex.class);
		job.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(job, new Path(inputPath)); // Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // Output settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(TextLongPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class); // Class settings
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);
		
		return job;
	}
	
	/**
	 * Prints out the usages of this program in case the user
	 *  gave incorrect input
	 */
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> <number of reduce tasks [default = 27]>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	/**
	 * Runs the main program
	 * @param args: String array of arguments given at start 
	 * @return -1 in case of error | 0 in case of success
	 * @throws Exception from the createJob() and the waitForCompletion() methods
	 */
	public int run(String[] args) throws Exception {
		int reduceTasks = 0;
		String inputPath = "", outputPath = "";
		
		// Set arguments
		if (args.length < 2) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputPath = args[0];
		outputPath = args[1];
		if (args.length >= 3) {
			try {
				reduceTasks = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Error: expected Integer instead of " + args[2]);
				return printUsage();
			}
		}
		else {
			reduceTasks = REDUCE_TASKS;
		}
		
		// Create and start job
		Job job = createJob(getConf(), inputPath, outputPath, reduceTasks);
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
		System.exit(result);
	}
}
