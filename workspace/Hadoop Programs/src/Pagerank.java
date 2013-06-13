import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import types.Node;

public class Pagerank {
	
	public static final int REDUCE_TASKS = 5;
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n");
			for(String line : lines){
				Node node = new Node(line);
				double partialPagerankMass = node.getPagerank() / node.adjacencyListSize();
				context.write(new LongWritable(node.getNodeId()), new Text(node.structure()));//Send structure of node and its neighbours
				Iterator<Node> adjacencyListIterator = node.getAdjacencyList().iterator();
				while(adjacencyListIterator.hasNext()) //Send each neighbour this base node has, with a partial pagerank mass from the base node
					context.write(new LongWritable(adjacencyListIterator.next().getNodeId()), new Text(Double.toString(partialPagerankMass))); 
			}
		}
	}

	public static class Partition extends Partitioner<IntWritable, Text>{
		public int getPartition(IntWritable nodeId, Text structure, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			return nodeId.get() % numPartitions;
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double pagerank = 0.0;
			Node destinationNode = null;
			for(Text value : values){
				Node node = new Node(key.get() + "  " + value.toString());
				if(node.containsList()){ //graph structure found
					destinationNode = node;
				} else { //Increment current nodes pagerank with received pagerank from other pages
					pagerank += node.getPagerank();
				}
			}
			destinationNode.setPagerank(pagerank);
			context.write(new LongWritable(destinationNode.getNodeId()), new Text(destinationNode.structure()));
		}
	}
	
	public static void printUsage(int argLength){
		if(argLength < 2) {
			System.out.println("usage:\t <input path> <output path> <number of reduce tasks [default 5]>");
			System.exit(-1);
		}
	}
	
	public static Job createJob(String[] args, Configuration conf) throws IOException {
		printUsage(args.length);
		conf.set("wiki.language", "en");
		
		Job job = new Job(conf, "Pagerank");
		job.setJarByClass(Pagerank.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));	//Input settings
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	//Ouput settings
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);							//Class settings
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		
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