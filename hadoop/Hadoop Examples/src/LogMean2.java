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
 * @version two
 * @date 25-03-2013
 */
public class LogMean2 {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(string t, integer r)
	 * 		Emit(string t, pair (r, 1))
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String k = tokenizer.nextToken();
				int time = Integer.parseInt(tokenizer.nextToken());
				String t = time + " 1";
				output.collect(new Text(k), new Text(t));
			}
		}
	}
	
	/** -PseudoExample-
	 * class Combiner
	 * 	method Combine(string t, pairs [(s1 , c1 ), (s2 , c2 ) . . .])
	 * 		sum ← 0
	 * 		cnt ← 0
	 * 		for all pair (s, c) ∈ pairs [(s1 , c1 ), (s2 , c2 ) . . .] do
	 * 			sum ← sum + s
	 * 			cnt ← cnt + c
	 * 		Emit(string t, pair (sum, cnt))
	 */
	public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int sum = 0, count = 0;
			String sumCountPair = "";
			while (values.hasNext()) {
				sumCountPair = values.next().toString();
				StringTokenizer tokenizer = new StringTokenizer(sumCountPair);
				sum += Integer.parseInt(tokenizer.nextToken());
				count += Integer.parseInt(tokenizer.nextToken());
			}
			sumCountPair = sum + " " + count;
			output.collect(key, new Text(sumCountPair));
		}
		
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(string t, pairs [(s1 , c1 ), (s2 , c2 ) . . .])
	 * 		sum ← 0
	 * 		cnt ← 0
	 * 		for all pair (s, c) ∈ pairs [(s1 , c1 ), (s2 , c2 ) . . .] do
	 * 			sum ← sum + s
	 * 			cnt ← cnt + c
	 * 		ravg ← sum/cnt
	 * 		Emit(string t, integer ravg )
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0, count = 0;
			String sumCountPair = "";
			while (values.hasNext()) {
				sumCountPair = values.next().toString();
				StringTokenizer tokenizer = new StringTokenizer(sumCountPair);
				sum += Integer.parseInt(tokenizer.nextToken());
				count += Integer.parseInt(tokenizer.nextToken());
			}
			int mean = sum / count;
			output.collect(key, new IntWritable(mean));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(LogMean2.class);
		job.setJobName("LogMean - Example 2");
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (args.length == 4) {
			job.setNumMapTasks(Integer.parseInt(args[0]));
			job.setNumReduceTasks(Integer.parseInt(args[1]));
			FileInputFormat.setInputPaths(job, new Path(args[2]));
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
		} else if (args.length < 4) {
			System.out.println("To few arguments given:\n");
			System.out.println("How to\n" +
					"*\tEstimation of Map-task\n" +
					"*\tNumber of Reduce-tasks\n" +
					"*\tInput file path\n" +
					"*\tOutput file path");
			System.exit(1);
		} else { //Case when more than 4 arguments given: incorrect
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
