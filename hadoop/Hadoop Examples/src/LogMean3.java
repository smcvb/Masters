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
 * @version three
 * @date 25-03-2013
 */
public class LogMean3 {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Initialize
	 * 		S ← new AssociativeArray
	 * 		C ← new AssociativeArray
	 * 
	 * 	method Map(string t, integer r)
	 * 		S{t} ← S{t} + r
	 * 		C{t} ← C{t} + 1
	 * 
	 * 	method Close
	 * 		for all term t ∈ S do
	 * 			Emit(term t, pair (S{t}, C{t}))
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		private Text log;
		private Text mean;
		private HashMap<String, String> logMap = null;
		private OutputCollector<Text, Text> output = null;
		
		@Override
		public void configure(JobConf job) { //Initialize
			log = new Text();
			mean = new Text();
			logMap = new HashMap<String, String>();
		}
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
			if (output == null) {
				output = out;
			}
			
			int time = 0, counter = 0;
			String line = value.toString(), v = "";
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String k = tokenizer.nextToken();
				if ((v = logMap.remove(k)) == null) {
					time = Integer.parseInt(tokenizer.nextToken());
					v = time + " 1";
					logMap.put(k, v);
				} else {
					StringTokenizer valueTokenizer = new StringTokenizer(v);
					time = Integer.parseInt(tokenizer.nextToken()) + Integer.parseInt(valueTokenizer.nextToken());
					counter = Integer.parseInt(valueTokenizer.nextToken()) + 1;
					v = time + " " + counter;
					logMap.put(k, v);
				}
			}
		}
		
		@Override
		public void close() throws IOException { //Close
			for (Entry<String, String> entry : logMap.entrySet()) {
				log.set(entry.getKey());
				mean.set(entry.getValue());
				output.collect(log, mean);
			}
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
		JobConf job = new JobConf(LogMean3.class);
		job.setJobName("LogMean - Example 3");
		job.setMapperClass(Map.class);
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
