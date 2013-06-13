import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextLongPair;

public class InvertedIndex {
	
	public static final int REDUCE_TASKS = 27;
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
		};
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, TextLongPair, IntWritable>{
		
		private long currentDocid, previousDocid;
		private HashMap<String, Integer> postingTupleMap;
		
		public void setup(Context context){
			currentDocid = 0;
			previousDocid = 0;
			postingTupleMap = new HashMap<String, Integer>();
		}
		
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			if(value.isEmpty())
				return;
			String term = "",
				contents = value.getContent(); //List containing terms to count frequency over
			currentDocid = Long.parseLong(value.getDocid());
			String[] terms = contents.split("\\s+");
			for(int i = 0; i < terms.length; i++){ //Payload part of the inverted indexing algorithm | term frequency in this case
				term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
				if(!term.equals("") && !isStopword(term))
					postingTupleMap.put(term, (postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1)); 
			}
			if(currentDocid != previousDocid){ //True: Finished a page, send to reducers
				for(Entry<String, Integer> entry : postingTupleMap.entrySet()){
					TextLongPair tuple = new TextLongPair(entry.getKey(), currentDocid);
					IntWritable frequency = new IntWritable(entry.getValue());
					context.write(tuple, frequency);
				}
				postingTupleMap.clear();//empty memory
			}
			previousDocid = currentDocid;
		}
		
		private boolean isStopword(String term){
			for(String stopword : STOPWORDS){
				if(term.equals(stopword))
					return true; 
			}
			return false;
		}
				 
		public void cleanup(Context context) throws IOException, InterruptedException { //clean last file
			for(Entry<String, Integer> entry : postingTupleMap.entrySet()){
				TextLongPair tuple = new TextLongPair(entry.getKey(), previousDocid);
				IntWritable frequency = new IntWritable(entry.getValue());
				context.write(tuple, frequency);
			}
			postingTupleMap.clear();//empty memory
		}
	}
	
	public static class SortComparator extends WritableComparator {
		
		protected SortComparator() {
			super(TextLongPair.class, true);
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
		
		protected GroupComparator() {
			super(TextLongPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextLongPair tp1 = (TextLongPair) w1;
			TextLongPair tp2 = (TextLongPair) w2;
			return tp1.compareTo(tp2);
		}
	}

	public static class Partition extends Partitioner<TextLongPair, IntWritable> {
		public int getPartition(TextLongPair tuple, IntWritable count, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			return Math.abs(tuple.getTerm().hashCode()) % numPartitions;
		}
	}
	
	public static class Reduce extends Reducer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable>{
		
		private String currentTerm, previousTerm;
		private ArrayList<TextIntPair> postingsList;
		private TextIntPairArrayWritable writablePostings;
		
		public void setup(Context context){
			currentTerm = "";
			previousTerm = null;
			postingsList = new ArrayList<TextIntPair>();
			writablePostings = new TextIntPairArrayWritable(TextIntPair.class);
		}
				
		public void reduce(TextLongPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			currentTerm = key.getTerm().toString();
			if(!currentTerm.equals(previousTerm) && previousTerm != null){ //new term start, write out old term
				TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
				postingsArray = postingsList.toArray(postingsArray);
				writablePostings.set(postingsArray);
				context.write(new Text(previousTerm), writablePostings);
				postingsList.clear();//empty memory
			}
			TextIntPair posting = new TextIntPair(new Text(key.getDocid().toString()), new IntWritable(values.iterator().next().get()));
			postingsList.add(posting);
			previousTerm = currentTerm;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException { //clean last term
			TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
			postingsArray = postingsList.toArray(postingsArray);
			writablePostings.set(postingsArray);
			context.write(new Text(currentTerm), writablePostings);
			postingsList.clear();//empty memory
		}
	}
	
	public static void printUsage(int argLength){
		if(argLength < 2) {
			System.out.println("usage:\t <input path> <output path> <number of reduce tasks [default 27]>");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException {
		printUsage(args.length);
		conf.set("wiki.language", "en");
		
		//conf.set("mapred.task.timeout", "2400000");
		conf.set("mapred.task.timeout", "0");
		conf.set("io.sort.mb", "512");
	    
		Job job = new Job(conf, "Inverted Indexing");
		job.setJarByClass(InvertedIndex.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));	//Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	//Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(TextLongPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);							//Class settings
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(Partition.class);
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
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}