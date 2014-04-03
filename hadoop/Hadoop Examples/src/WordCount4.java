import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

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
 * WordCount example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version three
 * @date 21-03-2013
 */
public class WordCount4 {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Initialize
	 * 		H ← new AssociativeArray
	 * 
	 * 	method Map(docid a, doc d)
	 * 		for all term t ∈ doc d do
	 * 			H{t} ← H{t} + 1
	 * 
	 * 	method Close
	 * 		for all term t ∈ H do
	 * 			Emit(term t, count H{t}) 
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Text word = new Text();
		private IntWritable counter = new IntWritable();
		private HashMap<String, Integer> wordMap = null;
		
		public void setup(Context context) { //initialize
			word = new Text();
			counter = new IntWritable();
			wordMap = new HashMap<String, Integer>();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
			Integer i = new Integer(0);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String w = tokenizer.nextToken().toLowerCase();
					
				if((i = wordMap.remove(w)) == null){
					wordMap.put(w, new Integer(1));
				} else {
					wordMap.put(w, (i.intValue() + 1));
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException { //close
			for(Entry<String, Integer> entry : wordMap.entrySet()){
				word.set(entry.getKey());
				counter.set(entry.getValue());
				context.write(word, counter);
			}
		}
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(term t, counts [c1 , c2 , . . .])
	 * 		sum ← 0
	 * 		for all count c ∈ counts [c1 , c2 , . . .] do
	 * 			sum ← sum + c
	 * 		Emit(term t, count sum)
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable reducedValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			reducedValue.set(sum);
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordCount4.class);
		job.setJobName("WordCount - Example 4");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
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
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}