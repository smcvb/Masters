import java.io.IOException;
import java.util.Iterator;

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

/**
 * Parallel Breadth First Search example
 * 
 * 	Based on pseudocode from:
 * 	"Data-Intensive Text Proccesing with MapReduce" 
 * 	From: Jimmy Lin and Chris Dyer
 *
 * @author stevenb
 * @version one
 * @date 03-04-2013
 */
public class ParallelBreadthFirstSearch {
	
	/** -PseudoExample-
	 * class Mapper
	 * 	method Map(nid n, node N )
	 * 		d ← N.Distance
	 * 		Emit(nid n, N )					//Pass along graph structure
	 * 		for all nodeid m ∈ N.AdjacencyList do
	 * 			Emit(nid m, d + w)			//Emit distances to reachable nodes
	 */
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String list = value.toString();
			String[] nodes = list.split("\n");
			for(int i = 0; i < nodes.length; i++){
				Node node = new Node(nodes[i]);
				int sourceDistance = node.getDistance();
				context.write(new IntWritable(node.getNodeId()), new Text(node.structure()));
				if(sourceDistance >= 0){	//non traversed node
					Iterator<Node> adjacencyListIterator = node.getAdjacencyList().iterator();
					while(adjacencyListIterator.hasNext()){
						Node neighbour = adjacencyListIterator.next();
						int distance = neighbour.getDistance() + sourceDistance;
						context.write(new IntWritable(neighbour.getNodeId()), new Text(Integer.toString(distance)));
					}
				}
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
	
	
	/** -PseudoExample-
	 * class Reducer
	 * 	method Reduce(nid m, [d1 , d2 , . . .])
	 * 		dmin ← ∞
	 * 		M ←∅
	 * 		for all d ∈ counts [d1 , d2 , . . .] do
	 * 			if IsNode(d) then
	 * 				M ←d
	 * 			else if d < dmin then
	 * 				dmin ← d
	 * 		M.Distance ← dmin
	 * 		Emit(nid m, node M )
	 */
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		private int sourceNode;
		
		public void setup(Context context){
			sourceNode = 1;//Changable through context, but static in this example
		}
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int minDistance = Integer.MAX_VALUE;
			Node destinationNode = null;
			for(Text value : values){
				Node node = new Node(key.get() + "  " + value.toString());
				if(node.containsList()){ //graph structure
					destinationNode = node;
				} else if(node.getDistance() < minDistance){
					minDistance = node.getDistance();
				}
			}
			if(minDistance != Integer.MAX_VALUE && destinationNode.getNodeId() != sourceNode)
				destinationNode.setDistance(minDistance);
			context.write(new IntWritable(destinationNode.getNodeId()), new Text(destinationNode.structure()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(ParallelBreadthFirstSearch.class);
		job.setJobName("Parallel Breadth First Search - Example 1");
		job.setMapperClass(Map.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
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