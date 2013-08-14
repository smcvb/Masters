
import info.bliki.wiki.filter.PlainTextConverter;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextPair;

public class InvertedIndex {
	
	public static final int REDUCE_TASKS = 5;
	
	public static class Map extends Mapper<LongWritable, WikipediaPage, Text, IntWritable>{
		
		/*private String currentFilename, previousFilename;
		private HashMap<String, Integer> postingTupleMap;
		
		public void setup(Context context){
			currentFilename = "";
			previousFilename = null;
			postingTupleMap = new HashMap<String, Integer>();
		}*/
		
		int count = 0;
		
		public void map(LongWritable key, WikipediaPage value, Context context) throws IOException, InterruptedException {
			count++;
			//System.out.printf("New page found - ");
			//System.out.printf("Number: %d\n", count);
			System.out.printf("Article: %s\n", value.getTitle());
			context.write(new Text(value.getTitle()), new IntWritable(count));
		}
		
		/*public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit file = (FileSplit)context.getInputSplit();
			currentFilename = file.getPath().getName().toString();
			String term = "",
					line = value.toString();'mapreduce'
			String[] terms = line.split("\\s+");
			for(int i = 0; i < terms.length; i++){ //Payload part of the inverted indexing algorithm | term frequency in this case
				term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
				if(!term.equals(""))
					postingTupleMap.put(term, (postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1)); 
			}
			if(!currentFilename.equals(previousFilename) && previousFilename != null){ //True: Finished a (split) file, send to reducers 
				for(Entry<String, Integer> entry : postingTupleMap.entrySet()){
					TextPair tuple = new TextPair(entry.getKey(), currentFilename);
					IntWritable frequency = new IntWritable(entry.getValue());
					context.write(tuple, frequency);
				}
				postingTupleMap.clear();//empty memory
			}
			previousFilename = currentFilename;
		}
		 
		public void cleanup(Context context) throws IOException, InterruptedException { //clean last file
			for(Entry<String, Integer> entry : postingTupleMap.entrySet()){
				TextPair tuple = new TextPair(entry.getKey(), previousFilename);
				IntWritable frequency = new IntWritable(entry.getValue());
				context.write(tuple, frequency);
			}
		}*/
	}
	
	/*public static class SortComparator extends WritableComparator {
		
		protected SortComparator() {
			super(TextPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextPair tp1 = (TextPair) w1;
			TextPair tp2 = (TextPair) w2;
			int cmp = tp1.getTerm().compareTo(tp2.getTerm());
			if (cmp != 0) {
				return cmp;
			}
			return tp1.getDocid().compareTo(tp2.getDocid());
		}
	}'mapreduce'
	
	public static class GroupComparator extends WritableComparator {
		
		protected GroupComparator() {
			super(TextPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextPair tp1 = (TextPair) w1;
			TextPair tp2 = (TextPair) w2;
			return tp1.compareTo(tp2);
		}
	}

	public static class Partition extends Partitioner<TextPair, IntWritable>{
		public int getPartition(TextPair tuple, IntWritable count, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			Text term = tuple.getTerm();
			return ((term.hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
	}*/
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		/*private String currentTerm, previousTerm;
		private ArrayList<TextIntPair> postingsList;
		private TextIntPairArrayWritable writablePostings;
		
		public void setup(Context context){
			currentTerm = "";
			previousTerm = null;
			postingsList = new ArrayList<TextIntPair>();
			writablePostings = new TextIntPairArrayWritable(TextIntPair.class);
		}*/
		
		int count = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			count++;
			int mapcountert = 0;
			System.out.printf("New page found with Iterator - ");
			System.out.printf("Number: %d", count);
			for(IntWritable value : values){
				mapcountert += value.get();
			}
			context.write(key, new IntWritable(mapcountert));
		}
				
		/*public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
		}*/
	}
	
	public static void printUsage(int argLength){
		if(argLength < 2) {
			System.out.println("usage:\t <input path> <output path> <number of reduce tasks [default 5]>");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException {
		printUsage(args.length);
		
		PlainTextConverter test = new PlainTextConverter();
		System.out.printf("Test op PlainTextConverter %s\n", test.toString());
		
	    conf.set("wiki.language", "en");
	    conf.set("mapred.map.child.java.opts", "-Xmx4096m");
	    conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
	    
		Job job = new Job(conf, "Inverted Indexing");
		job.setJarByClass(InvertedIndex.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));	//Input settings
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	//Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputKeyClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);							//Class settings
		//job.setSortComparatorClass(SortComparator.class);
		//job.setGroupingComparatorClass(GroupComparator.class);
		//job.setPartitionerClass(Partition.class);
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