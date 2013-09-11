import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import types.TextStripe;

/**
 * Co-occurence Matrix example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version two
 * @date 26-03-2013
 */
public class CooccurenceMatrix2 {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(docid a, doc d)
	 * 		for all term w ∈ doc d do
	 * 			H ← new AssociativeArray
	 * 			for all term u ∈ Neighbors(w) do
	 * 				H{u} ← H{u} + 1
	 * 			Emit(Term w, Stripe H)
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, TextStripe> {
		
		private Text word = new Text();
		private TextStripe stripe = new TextStripe();
		private int window = 1;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String neighbour = "", current = "", line = value.toString();
			String[] terms = line.split("\\s");
			for (int i = 0; i < terms.length; i++) {
				current = terms[i];
				stripe.clear();
				if (terms.length == 0) {
					break;
				}
				
				for (int j = i - window; j < i + window + 1; j++) {
					if (j == i || j < 0) {
						continue;
					}
					if (j >= terms.length) {
						break;
					}
					neighbour = terms[j].toLowerCase();
					if (stripe.containsKey(new Text(neighbour))) {
						stripe.increment(new Text(neighbour));
					} else {
						stripe.put(new Text(neighbour), new IntWritable(1));
					}
				}
				word.set(current);
				context.write(word, stripe);
			}
		}
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(term w, stripes [H1 , H2 , H3 , . . .])
	 * 		Hf ← new AssociativeArray
	 * 		for all stripe H ∈ stripes [H1 , H2 , H3 , . . .] do
	 * 			Sum(Hf , H)
	 * 		Emit(term w, stripe Hf )
	 */
	public static class Reduce extends Reducer<Text, TextStripe, Text, TextStripe> {
		
		private TextStripe completeStripe = new TextStripe();
		
		@Override
		public void reduce(Text key, Iterable<TextStripe> values, Context context) throws IOException, InterruptedException {
			for (TextStripe stripe : values) {
				completeStripe.addStripe(stripe);
			}
			//System.out.printf("%s || %s\n", key.toString(), completeStripe.toString());
			context.write(key, completeStripe);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(CooccurenceMatrix2.class);
		job.setJobName("CooccurenceMatrix - Example 2");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextStripe.class);
		
		if (args.length == 3) {
			job.setNumReduceTasks(Integer.parseInt(args[0]));
			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
		} else if (args.length < 3) {
			System.out.println("To few arguments given:\n");
			System.out.println("How to\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file\n" +
					"*\tOutput file");
			System.exit(1);
		} else { //Case when more than 4 arguments given: incorrect
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
