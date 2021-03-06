import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.Node;

/**
 * Hadoop program to run the Pagerank algorithm as
 *  specified in 'Data-Intensive Text Processing with MapReduce'
 * @author stevenb
 * @date 05-08-2013
 */
public class Pagerank extends Configured implements Tool {
	
	public static enum MapCounters { // Counters used for the Map tasks
		NODES, DANGLING_NODES, STRUCTURES,
		MASS_WRITTEN, TOTAL_WRITES
	}
	
	public static enum ReduceCounters { // Counters used for the Reduce tasks
		NODES, DANGLING_NODES, STRUCTURES,
		MASS_READ, TOTAL_READS
	}
	
	public static final double ALPHA = 0.15f; // Global double representing the damping factor of the Pagerank algorithm
	public static final float CONVERGENCE_POINT = 0.001f;
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Node> {
		
		private HashMap<LongWritable, Double> massMessagesMap;
		
		@Override
		public void setup(Context context) {
			massMessagesMap = new HashMap<LongWritable, Double>();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Node node = new Node();
			LongWritable[] links;
			double partialPagerankMass = 0.0f;
			
			String[] lines = value.toString().split("\n");
			for (String line : lines) { // Start reading in a line and converting it to a complete Node object
				String terms[] = line.split("\\s+");
				node.setNodeId(Long.parseLong(terms[0]));
				node.setPagerank(Double.parseDouble(terms[1]));
				links = new LongWritable[terms.length - 2];
				if (links.length > 0) {
					for (int i = 2; i < terms.length; i++) {
						links[i - 2] = new LongWritable(Long.parseLong(terms[i]));
					}
					node.setAdjacencyList(links);
					context.getCounter(MapCounters.STRUCTURES).increment(1); // New node containing a structure found
				}
				else {
					context.getCounter(MapCounters.DANGLING_NODES).increment(1); // New node containing no structure found; thus Dangling-node
				}
				node.setAsCompleteNode();
				
				Node structure = new Node(); // Extract the structure from the Node object to send to the Reduce job
				structure.setNodeId(node.getNodeId());
				structure.setAdjacencyList(node.getAdjacencyList());
				structure.setAsStructureNode();
				context.write(structure.getNodeId(), structure);
				context.getCounter(MapCounters.TOTAL_WRITES).increment(1); // New write done
				
				/** 
				 * Create the mass messages to be send to the neighbors
				 * Dangling nodes will not have a structure, since these
				 *  exist from a NodeId and Pagerank only, thus need to 
				 *  check whether the node contains a structure
				 * Also, do in-mapper combining by moving the messages to an
				 *  HashMap object and so lower the number of messages to be 
				 *  send to the Reduce jobs
				 */
				if (node.hasStructure()) {
					partialPagerankMass = node.getPagerank().get() / links.length;
					for (LongWritable link : links) {
						massMessagesMap.put(link, massMessagesMap.containsKey(link) ? massMessagesMap.get(link) + partialPagerankMass : partialPagerankMass);
					}
				}
				context.getCounter(MapCounters.NODES).increment(1); // Complete new node done
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(MapCounters.MASS_WRITTEN).increment(massMessagesMap.size()); // Number of Mass messages which will be send
			context.getCounter(MapCounters.TOTAL_WRITES).increment(massMessagesMap.size());
			for (Entry<LongWritable, Double> entry : massMessagesMap.entrySet()) { // Send partial Pagerank mass from this node to its neighbors
				Node mass = new Node();
				mass.setNodeId(entry.getKey());
				mass.setPagerank(entry.getValue());
				mass.setAsMassNode();
				context.write(mass.getNodeId(), mass);
			}
			massMessagesMap.clear(); // Clear HashMap to save memory
		}
	}
	
	public static class Partition extends Partitioner<LongWritable, Node> {
		
		@Override
		public int getPartition(LongWritable nodeId, Node node, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			return (int) (nodeId.get() % numPartitions);
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, Node, LongWritable, Text> {
		
		private double totalMass; // Global variable to store the mass found in every reduce() call
		
		@Override
		public void setup(Context context) {
			totalMass = 0.0f;
		}
		
		@Override
		public void reduce(LongWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
			long nodeId = key.get();
			double pagerank = 0.0f;
			LongWritable[] links = new LongWritable[0];
			
			for (Node node : values) {
				if (node.isMassNode()) { // Increment current nodes Pagerank with received Pagerank from other pages
					pagerank += node.getPagerank().get();
					context.getCounter(ReduceCounters.MASS_READ).increment(1);
				}
				else if (node.isStructureNode() && node.hasStructure()) { // Graph structure found
					String[] adjacencyStringList = node.getAdjacencyList().toStrings();
					links = new LongWritable[adjacencyStringList.length];
					for (int i = 0; i < adjacencyStringList.length; i++) {
						links[i] = new LongWritable(Long.parseLong(adjacencyStringList[i]));
					}
					context.getCounter(ReduceCounters.STRUCTURES).increment(1);
				}
				else if (node.isStructureNode()) { // Graph structure found, but not outgoing links; hence Dangling-node
					context.getCounter(ReduceCounters.DANGLING_NODES).increment(1);
				}
				context.getCounter(ReduceCounters.TOTAL_READS).increment(1); // New read done
			}
			
			totalMass += pagerank;
			
			Node destinationNode = new Node();
			destinationNode.setNodeId(nodeId);
			destinationNode.setPagerank(pagerank);
			destinationNode.setAdjacencyList(links);
			context.write(destinationNode.getNodeId(), new Text(destinationNode.structure()));
			context.getCounter(ReduceCounters.NODES).increment(1); // Complete new node done
		}
		
		@Override
		/**
		 *  Write to a file the amount of PageRank mass we've seen in this reducer
		 */
		public void cleanup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String taskId = conf.get("mapred.task.id");
			String path = conf.get("PageRankMassPath");
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
			out.writeDouble(totalMass);
			out.close();
		}
	}
	
	public static class MassDistributionMap extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private long numNodes;
		private double lostPagerankJuice, lostPagerankJuicePart;
		
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			numNodes = conf.getInt("numNodes", 0);
			lostPagerankJuice = conf.getFloat("missingMass", 0.0f);
			lostPagerankJuicePart = lostPagerankJuice / numNodes;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Node node = new Node();
			LongWritable[] links;
			double pagerank = 0.0f;
			String[] lines = value.toString().split("\n");
			for (String line : lines) {
				String terms[] = line.split("\\s+");
				node.setNodeId(Long.parseLong(terms[0]));
				pagerank = Double.parseDouble(terms[1]);
				
				links = new LongWritable[terms.length - 2];
				if (links.length > 0) {
					for (int i = 2; i < terms.length; i++) {
						links[i - 2] = new LongWritable(Long.parseLong(terms[i]));
					}
					node.setAdjacencyList(links);
					context.getCounter(MapCounters.STRUCTURES).increment(1); // New structure found
				}
				else {
					context.getCounter(MapCounters.DANGLING_NODES).increment(1); // No links found, hence a dangling node
				}
				
				double jump = ALPHA / numNodes; // Finalize the computation of the Pagerank iteration and add a part of the missing mass
				double link = (1 - ALPHA) * (pagerank + lostPagerankJuicePart);
				pagerank = jump + link;
				node.setPagerank(pagerank);
				context.write(node.getNodeId(), new Text(node.structure()));
				context.getCounter(MapCounters.TOTAL_WRITES).increment(1); // New write done
				context.getCounter(MapCounters.NODES).increment(1); // Complete new node done
			}
		}
	}
	
	/**
	 * Runs the second step of the Pagerank Algorithm
	 * Will distribute the lost Pagerank juice over all
	 *  the nodes and will adjust the Pagerank according
	 *  to the jump and link factor
	 * @param conf: the Configuration object for the Map/Reduce job
	 * @param basePath: String pointing to the base of the output files
	 * @param iteration: Integer containing the number of iterations the program will run
	 * 	used to adjust the Job and Path names accordingly
	 * @throws IOException for File adjustment and the starting of the job
	 * @throws InterruptedException for starting the job
	 * @throws ClassNotFoundException for starting the job
	 */
	public void phase2(Configuration conf, String basePath, int iteration) throws IOException, InterruptedException, ClassNotFoundException {
		String in = basePath + "/iter" + (iteration + 1) + "out";
		String out = basePath + "/iter" + (iteration + 1);
		
		Job phase2 = new Job(conf, "Pagerank - Iteration " + (iteration + 1) + " - Phase 2"); // Main settings
		phase2.setJarByClass(Pagerank.class);
		FileInputFormat.setInputPaths(phase2, new Path(in)); // Input settings
		phase2.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(phase2, new Path(out)); // Output settings
		phase2.setOutputFormatClass(TextOutputFormat.class);
		phase2.setOutputKeyClass(LongWritable.class);
		phase2.setOutputValueClass(Node.class);
		phase2.setMapperClass(MassDistributionMap.class); // Class settings
		phase2.setNumReduceTasks(0); // Set to zero, since no reduce tasks should be started
		
		long startTime = System.currentTimeMillis();
		if (phase2.waitForCompletion(true)) {
			System.out.println("Phase 2, Iteration " + (iteration + 1) + " Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
	}
	
	/**
	 * Runs the first step of the Pagerank Algorithm
	 * Map: Will read in all the nodes of the graph and 
	 *  partitions those nodes their Pagerank, afterwards
	 *  it will send these partitions to their neighbors
	 * Reduce: Counts all the messages received per node
	 *  and increments its Pagerank accordingly
	 * @param conf: the Configuration object for the Map/Reduce job
	 * @param startPath: String holding the initial input path
	 * @param basePath: String pointing to the base of the output files
	 * @param iteration: Integer containing the number of iterations the program will run
	 * 	used to adjust the Job and Path names accordingly
	 * @return the total mass found in the Reduce found
	 * @throws IOException for File adjustment and the starting of the job
	 * @throws InterruptedException for starting the job
	 * @throws ClassNotFoundException for starting the job
	 */
	public double phase1(Configuration conf, String startPath, String basePath, int iteration) throws IOException, InterruptedException, ClassNotFoundException {
		String in = "";
		if (iteration == 0) { // If first iteration, use start path given
			in = startPath;
		}
		else { // else, use previous basePath
			in = basePath + "/iter" + iteration;
		}
		String out = basePath + "/iter" + (iteration + 1) + "out";
		String outm = out + "-mass"; //Path 'outm' used for the total mass in the reduce phase
		conf.set("PageRankMassPath", outm);
		int numPartitions = 0; // Count the number of file partitions, so the number of reduce tasks can equal the number of map tasks
		for (FileStatus s : FileSystem.get(conf).listStatus(new Path(startPath))) {
			if (s.getPath().getName().contains("part-")) {
				numPartitions++;
			}
		}
		
		Job phase1 = new Job(conf, "Pagerank - Iteration " + (iteration + 1) + " - Phase 1"); // Main settings
		phase1.setJarByClass(Pagerank.class);
		FileInputFormat.setInputPaths(phase1, new Path(in)); // Input settings
		phase1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(phase1, new Path(out)); // Output settings
		phase1.setOutputFormatClass(TextOutputFormat.class);
		phase1.setOutputKeyClass(LongWritable.class);
		phase1.setOutputValueClass(Node.class);
		phase1.setMapperClass(Map.class); // Class settings
		phase1.setReducerClass(Reduce.class);
		phase1.setPartitionerClass(Partition.class);
		phase1.setNumReduceTasks(numPartitions);
		
		long startTime = System.currentTimeMillis();
		if (phase1.waitForCompletion(true)) {
			System.out.println("Phase 1, Iteration " + (iteration + 1) + " Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		
		boolean done = false;
		int retry = 0;
		double mass = 0.0; // Retrieve the mass written to the 'outm' Path in the reduce step of phase 1
		FileSystem fs = FileSystem.get(conf);
		for (FileStatus f : fs.listStatus(new Path(outm))) {
			while (!done && retry < 5) {
				try {
					FSDataInputStream fin = fs.open(f.getPath());
					mass += fin.readDouble();
					fin.close();
					done = true;
					retry = 0;
				} catch (Exception e) {
					System.out.printf("Unsuccesfully read a file (%s) | will retry...\n", e);
					done = false;
					retry++;
				}
				
			}
			done = false; // Reset done to false for next round
			if (retry >= 1) {
				System.out.printf("\nUnsuccesfully read a file for five times | File has been skipped\n\n");
				retry = 0; // Reset retry to zero for next round
			}
		}
		return mass;
	}
	
	/**
	 * Will iterate over the two phases in order for the
	 *  specified number of iterations
	 * @param conf: Configuration object used for every Map/Reduce task initiated
	 * @param startPath: the initial input path as a String
	 * @param basePath: the base path as a String to put all the output and subsequently input files in
	 * @param iterations: the number of iterations to run the algorithm as an Integer
	 * @throws Exception from the phase1() and phase2() methods for starting 
	 * 		MapReduce jobs and opening/closing files
	 */
	private void iterate(Configuration conf, String startPath, String basePath, int iterations) throws Exception {
		double mass = 0.0, // Used to store the mass send around in phase 1 
		missingMass = 0.0, // Will hold the data missed in phase 1 and given to phase 2 to give an equal share to every node
		oldMass = 0.0; // Stores the previously found mass send around to check convergence
		
		System.out.printf("Startpath: %s Basepath: %s Iterations: %d\n", startPath, basePath, iterations);
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < iterations; i++) {
			if (oldMass != 0) {
				System.out.printf("Previous send mass: %f and current send mass: %f\n", oldMass, mass);
				if (Math.abs(oldMass - mass) < CONVERGENCE_POINT) { // If this holds, convergence is being reached, hence break
					System.out.printf("Convergence point has been reached in iterations %d\n\toldMass: %f - mass: %f = diff: %f\n", i - 1, oldMass, mass, oldMass - mass);
					break;
				}
			}
			oldMass = mass;
			mass = 0.0f;
			missingMass = conf.getInt("numNodes", 0);
			
			System.out.printf("Phase 1, Iteration %d will start\nNumber of Nodes: %d Total Mass Send: %f Missing Mass: %f\n\n;", i + 1, conf.getInt("numNodes", 0), mass, missingMass);
			mass = phase1(conf, startPath, basePath, i);
			
			missingMass -= mass; // The missing mass will equal the total mass (thus, number of nodes) minus the mass found in phase 1
			System.out.printf("\n\nPhase 1, Iteration %d complete\nNumber of Nodes: %d Total Mass Send: %f Missing Mass: %f\nWill start phase 2, Iteration %d\n\n", i + 1, conf.getInt("numNodes", 0), mass, missingMass, i + 1);
			conf.setFloat("missingMass", (float) missingMass);
			phase2(conf, basePath, i);
			System.out.printf("\n\nPhase 2, Iteration %d complete\n", i + 1);
		}
		System.out.println("\n\nJob finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
	
	/**
	 * Prints out the usages of this program in case the user
	 *  gave incorrect input
	 */
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> <number of nodes> <number of iterations>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	/**
	 * Runs the main program
	 * @param args: String array of arguments given at start 
	 * @return -1 in case of error | 0 in case of success
	 * @throws Exception from the iterate() method
	 */
	@Override
	public int run(String[] args) throws Exception {
		int numNodes = 0, iterations = 0;
		String startPath = "", basePath = "";
		Configuration conf = new Configuration(getConf());
		
		// Set arguments
		if (args.length < 4) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		startPath = args[0];
		basePath = args[1];
		try {
			numNodes = Integer.parseInt(args[2]);
			conf.setInt("numNodes", numNodes);
			iterations = Integer.parseInt(args[3]);
		} catch (NumberFormatException e) {
			System.err.println("Error: expected Integers instead of " + args[2] + " (arg 2) and " + args[3] + " (arg 3)");
			return printUsage();
		}
		
		// Create and start iterations
		iterate(conf, startPath, basePath, iterations);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new Pagerank(), args);
		System.exit(result);
	}
}
