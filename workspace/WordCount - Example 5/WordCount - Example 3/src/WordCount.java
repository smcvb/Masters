import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

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
public class WordCount {
		
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
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text word = new Text();
		private IntWritable counter = new IntWritable();
		private HashMap<String, Integer> wordMap = null;
		private OutputCollector<Text, IntWritable> output = null;
		
		public void configure(JobConf job) { //initialize
			word = new Text();
			counter = new IntWritable();
			wordMap = new HashMap<String, Integer>();
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> out, Reporter reporter) throws IOException {
			if(output == null)
				output = out;
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
		
		public void close() throws IOException { //close
			for(Entry<String, Integer> entry : wordMap.entrySet()){
				word.set(entry.getKey());
				counter.set(entry.getValue());
				output.collect(word, counter);
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
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable reducedValue = new IntWritable();
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			reducedValue.set(sum);
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(WordCount.class);
		job.setJobName("WordCount - Example 3");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if(args.length == 4){
			job.setNumMapTasks(Integer.parseInt(args[0]));
			job.setNumReduceTasks(Integer.parseInt(args[1]));
			FileInputFormat.setInputPaths(job, new Path(args[2]));
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
		} else if(args.length < 4){
			System.out.println("To few arguments given:\n");
			System.out.println("How to\n" +
					"*\tEstimation of Map-task\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file\n" +
					"*\tOutput file");
			System.exit(1);
		} else {	//Case when more than 4 arguments given: incorrect
			System.out.println("To many arguments given:\n");
			System.out.println("How to\n" +
					"*\tEstimation of Map-task\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file\n" +
					"*\tOutput file");
			System.exit(1);
		}
		
		JobClient.runJob(job);
	}
}
