import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import types.NodePR;

/**
 * Page Rank example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version one
 * @date 02-04-2013
 */
public class PageRank {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(nid n, node N )
	 * 		p ← N.PageRank/|N.AdjacencyList|
	 * 		Emit(nid n, N )			//Pass along graph structure
	 * 		for all nodeid m ∈ N.AdjacencyList do
	 * 			Emit(nid m, p)			//Pass PageRank mass to neighbors
	 */
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String list = value.toString();
			String[] nodes = list.split("\n");
			for (int i = 0; i < nodes.length; i++) {
				NodePR node = new NodePR(nodes[i]);
				double partialPagerankMass = node.getPagerank() / node.adjacencyListSize();
				context.write(new IntWritable(node.getNodeId()), new Text(node.structure()));
				Iterator<NodePR> adjacencyListIterator = node.getAdjacencyList().iterator();
				while (adjacencyListIterator.hasNext()) {
					context.write(new IntWritable(adjacencyListIterator.next().getNodeId()), new Text(Double.toString(partialPagerankMass)));
				}
			}
		}
	}
	
	public static class Partition extends Partitioner<IntWritable, Text> {
		
		@Override
		public int getPartition(IntWritable nodeId, Text structure, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			return nodeId.get() % numPartitions;
		}
	}
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(nid m, [p1 , p2 , . . .])
	 * 		M ←∅
	 * 		for all p ∈ counts [p1 , p2 , . . .] do
	 * 			if IsNode(p) then
	 * 				M ←p				//Recover graph structure
	 * 			else
	 * 				s←s+p			//Sum incoming PageRank contributions
	 * 		M.PageRank ← s
	 * 		Emit(nid m, node M )
	 */
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double pagerank = 0.0;
			NodePR destinationNode = null;
			for (Text value : values) {
				NodePR node = new NodePR(key.get() + "  " + value.toString());
				if (node.containsList()) { //graph structure
					destinationNode = node;
				} else {
					pagerank += node.getPagerank();
				}
			}
			destinationNode.setPagerank(pagerank);
			context.write(new IntWritable(destinationNode.getNodeId()), new Text(destinationNode.structure()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank - Example 1");
		job.setMapperClass(Map.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
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
