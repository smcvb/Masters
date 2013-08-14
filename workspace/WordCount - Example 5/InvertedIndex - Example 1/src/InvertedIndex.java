import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Inverted Index example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version one
 * @date 28-03-2013
 */
public class InvertedIndex {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(docid n, doc d)
	 * 		H ← new AssociativeArray
	 * 		for all term t ∈ doc d do
	 * 			H{t} ← H{t} + 1
	 * 		for all term t ∈ H do
	 * 			Emit(tuple t, n , tf H{t})
	 */
	public static class Map extends Mapper<LongWritable, Text, TextLongPair, IntWritable>{
		
		private HashMap<String, Integer> tupleMap = new HashMap<String, Integer>();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String term = "",
					line = value.toString();
			String[] terms = line.split("\\s");
			
			for(int i = 0; i < terms.length; i++){
				term = terms[i].toLowerCase();
				tupleMap.put(term, (tupleMap.containsKey(term) ? tupleMap.get(term) + 1 : 1)); 
			}
			
			for(Entry<String, Integer> entry : tupleMap.entrySet()){
				TextLongPair tuple = new TextLongPair(entry.getKey(), key.get());
				IntWritable frequency = new IntWritable(entry.getValue());
				System.out.printf("%s %s\n", tuple.toString(), frequency.toString());
				context.write(tuple, frequency);
			}
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

	public static class Partition extends Partitioner<TextLongPair, IntWritable>{
		public int getPartition(TextLongPair tuple, IntWritable count, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			Text term = tuple.getTerm();
			return ((term.hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Initialize
	 * 		tprev ← ∅
	 * 		P ← new PostingsList
	 * 
	 * 	method Reduce(tuple t, n , tf [f ])
	 * 		if t = tprev ∧ tprev = ∅ then
	 * 			Emit(term t, postings P )
	 * 			P.Reset()
	 * 		P.Add( n, f )
	 * 		tprev ← t
	 * 
	 * method Close
	 * 	Emit(term t, postings P )
	 */
	public static class Reduce extends Reducer<TextLongPair, IntWritable, Text, LongIntPairArrayWritable>{
		
		private String currentTerm, previousTerm;
		private LongIntPair post;
		private ArrayList<LongIntPair> postings;
		private LongIntPairArrayWritable writablePostings;
		
		public void setup(Context context){
			currentTerm = "";
			previousTerm = "";
			post = new LongIntPair();
			postings = new ArrayList<LongIntPair>();
			writablePostings = new LongIntPairArrayWritable(LongIntPair.class);
		}
		
		public void reduce(TextLongPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			currentTerm = key.getTerm().toString();
			if(!currentTerm.equals(previousTerm)){
				LongIntPair[] postingsArray = new LongIntPair[postings.size()];
				postingsArray = postings.toArray(postingsArray);
				writablePostings.set(postingsArray);
				context.write(new Text(previousTerm), writablePostings);
				System.out.printf("PREV and CUR aren't EQUAL!\n");
			}
			post.set(key.getDocid(), values.iterator().next());
			postings.add(post);
			previousTerm = currentTerm;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			LongIntPair[] postingsArray = new LongIntPair[postings.size()];
			postingsArray = postings.toArray(postingsArray);
			writablePostings.set(postingsArray);
			context.write(new Text(previousTerm), writablePostings);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		job.setJobName("InvertedIndex - Example 1");
		job.setMapperClass(Map.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(TextLongPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		if(args.length == 3){
			job.setNumReduceTasks(Integer.parseInt(args[0]));
			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
		} else if(args.length < 3){
			System.out.println("To few arguments given:\n");
			System.out.println("How to\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file\n" +
					"*\tOutput file");
			System.exit(1);
		} else {	//Case when more than 4 arguments given: incorrect
			System.out.println("To many arguments given:\n");
			System.out.println("How to\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file\n" +
					"*\tOutput file");
			System.exit(1);
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}