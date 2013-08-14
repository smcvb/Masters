import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * Co-occurence Matrix example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version four
 * @date 27-03-2013
 */
public class CooccurenceMatrix {
	
	/** -PseudoExample-
	 * class MAPPER
  	 * 	method INITIALIZE
	 * 		H = new hash map
	 *    
	 * 	method MAP(docid a, doc d)
	 * 		for all term w in doc d do
	 * 			for all term u patri neighbors(w) do
	 * 				H(w) = H(w) + 1
	 * 				emit(pair(u, w), count 1)
	 * 
	 * 	method CLOSE
	 * 		for all term w in H
	 * 		emit(pair(w, *), H(w))
	 */
	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable>{
		
		private int window;
		private Integer count;
		private HashMap<Entry<String, String>, Integer> wordMap;
		private SimpleImmutableEntry<String, String> pair, marginalPair;
		
		public void setup(Context context) { //initialize
			window = 1;
			count = 1;
			wordMap = new HashMap<Entry<String, String>, Integer>();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String word = "",
					neighbour = "",
					line = value.toString();
			String[] terms = line.split("\\s");
			
			for(int i = 0; i < terms.length; i++){
				if(terms.length == 0)
					break;
				
				word = terms[i].toLowerCase();
				marginalPair = new SimpleImmutableEntry<String, String>(word, "*");
				
				for(int j = i - window; j < (i + window + 1); j++){
					if(j == i || j < 0)
						continue;
					if(j >= terms.length)
						break;
					
					neighbour = terms[j].toLowerCase();
					pair = new SimpleImmutableEntry<String, String>(word, neighbour);
					
					if(wordMap.containsKey(pair)){
						count = wordMap.get(pair);
						wordMap.put(pair, (count + 1));
					} else {
						wordMap.put(pair, 1);
					}
					
					if(wordMap.containsKey(marginalPair)){
						count = wordMap.get(marginalPair);
						wordMap.put(marginalPair, (count + 1));
					} else {
						wordMap.put(marginalPair, 1);
					}
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException { //close
			SimpleImmutableEntry<String, String> immutablePair;
			TextPair pair = new TextPair();
			IntWritable counter = new IntWritable();
			for(Entry<Entry<String, String>, Integer> entry : wordMap.entrySet()){
				immutablePair = new SimpleImmutableEntry<String, String>(entry.getKey());
				pair.set(immutablePair.getKey(), immutablePair.getValue());
				counter.set(entry.getValue());
				context.write(pair, counter);
			}
		}
	}
	
	/** -PseudoExample-
	 * class SORTING_COMPARATOR
	 * 	method compare(key (p1, u1), key (p2, u2))
	 * 		if p1 = p2 AND u1 = *
	 * 			return key1 is lower
	 * 		if p1 = p2 AND u2 = *
	 * 			return key2 is lower 
	 * 		return compare(p1, p2)
	 */
	public static class Comparator extends WritableComparator {
		
		protected Comparator() {
			super(TextPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextPair tp1 = (TextPair) w1;
			TextPair tp2 = (TextPair) w2;
			int cmp = tp1.getFirst().compareTo(tp2.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			if(tp1.getSecond().toString().equals("*")){
				return -1;
			} else if(tp2.getSecond().toString().equals("*")){
				return 1;
			}
			return tp1.getSecond().compareTo(tp2.getSecond());
		}
	}
	
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

	
	/** -PseudoExample-
	 * class PARTITIONING_COMPARATOR
  	 * 	method compare(key (p1, u1), key (p2, u2))
	 * 		if p1 = p2
	 * 			return keys are equal
	 * 		return keys are different
	 */
	public static class Partition extends Partitioner<TextPair, IntWritable>{
		public int getPartition(TextPair pair, IntWritable count, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			Text first = pair.getFirst();
			return ((first.hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
	}
	
	/** -PseudoExample-
	 * class REDUCER
	 * 	variable total_occurrences = 0
	 * 	method REDUCE(pair (p, u), counts[c1, c2, ..., cn])
	 * 		s = 0
	 * 		for all c in counts[c1, c2, ..., cn] do
	 * 			s = s + c
	 * 			if u = *
	 * 				total_occurrences = s
	 * 			else
	 * 				emit(pair p, s/total_occurrences)
	 */
	public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, FloatWritable>{
		
		private float sum, marginal;
		
		/*public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			if(key.getSecond().toString().equals("*")){
				sum = 0;
				marginal = 0;
				for(IntWritable value : values){
					sum += value.get();
				}
				context.write(key, new FloatWritable(sum));
				marginal = sum;
			} else {
				sum = 0;
				for(IntWritable value : values){
					sum += value.get();
				}
				context.write(key, new FloatWritable(sum / marginal));
			}
		}*/
		
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			context.write(key, new FloatWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(CooccurenceMatrix.class);
		job.setJobName("CooccurenceMatrix - Example 4");
		job.setMapperClass(Map.class);
		job.setSortComparatorClass(Comparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(TextPair.class);
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