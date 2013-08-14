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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Co-occurence Matrix example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version one
 * @date 25-03-2013
 */
public class CooccurenceMatrix {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(docid a, doc d)
	 * 		for all term w ∈ doc d do
	 * 			for all term u ∈ Neighbors(w) do
	 * 			Emit(pair (w, u), count 1) //Emit count for each co-occurrence
	 */
	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable>{
		
		private TextPair cooccurence = new TextPair();
		private IntWritable count = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String prev = "",
					current = "",
					post = "",
					line = value.toString();
			String[] tokens = line.split("\\s");
			for(int i = 0; i < tokens.length; i++){
				current = tokens[i].toLowerCase();
				if(tokens.length > 1){
					if(i == tokens.length - 1){
						prev = tokens[i - 1].toLowerCase();
						cooccurence.set(current, prev);
						context.write(cooccurence, count);//left-current pair
					} else if(i == 0){
						post = tokens[i + 1].toLowerCase();
						cooccurence.set(current, post);
						context.write(cooccurence, count);//current-right pair
					} else {
						prev = tokens[i - 1].toLowerCase();
						post = tokens[i + 1].toLowerCase();
						cooccurence.set(current, prev);
						context.write(cooccurence, count);//left-current pair
						cooccurence.set(current, post);
						context.write(cooccurence, count);//current-right pair
					}
				}
			}
		}
	}
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(pair p, counts [c1 , c2 , . . .])
	 * 		s←0
	 * 		for all count c ∈ counts [c1 , c2 , . . .] do
	 * 			s←s+c 
	 * 		Emit(pair p, count s)
	 */
	public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable>{
		
		private IntWritable reducedCount = new IntWritable(0);
		
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			reducedCount.set(sum);
			context.write(key, reducedCount);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(CooccurenceMatrix.class);
		job.setJobName("CooccurenceMatrix - Example 1");
		job.setMapperClass(Map.class);
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