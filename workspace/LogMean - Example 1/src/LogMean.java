import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


/**
 * LogMean example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version one
 * @date 25-03-2013
 */
public class LogMean {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(string t, integer r)
	 * 		Emit(string t, integer r)
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String k = tokenizer.nextToken();
				int time = Integer.parseInt(tokenizer.nextToken());
				output.collect(new Text(k), new IntWritable(time));
			}
		}
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(string t, integers [r1 , r2 , . . .])
	 * 		sum ← 0
	 * 		cnt ← 0
	 * 		for all integer r ∈ integers [r1 , r2 , . . .] do
	 * 			sum ← sum + r
	 * 			cnt ← cnt + 1
	 * 		ravg ← sum/cnt
	 * 		Emit(string t, integer ravg ) 
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0,
					count = 0;
			while(values.hasNext()){
				sum += values.next().get();
				count++;
			}
			int mean = sum / count;
			output.collect(key, new IntWritable(mean));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(LogMean.class);
		job.setJobName("LogMean - Example 1");
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
					"*\tInput file path\n" +
					"*\tOutput file path");
			System.exit(1);
		} else {	//Case when more than 4 arguments given: incorrect
			System.out.println("To many arguments given:\n");
			System.out.println("How to\n" +
					"*\tEstimation of Map-task\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file path\n" +
					"*\tOutput file path");
			System.exit(1);
		}
		
		JobClient.runJob(job);
	}
}
