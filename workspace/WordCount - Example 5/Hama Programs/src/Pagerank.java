import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.SumAggregator;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * Hadoop program to run the Pagerank algorithm as
 *  specified in 'Data-Intensive Text Processing with MapReduce'
 * @author stevenb
 * @date 10-07-2013
 */
public class Pagerank extends Configured implements Tool {
	
	public static final float CONVERGENCE_POINT = 0.0001f;
	
	public static class PageRankBSP extends Vertex<LongWritable, NullWritable, DoubleWritable> {
		
		public static final double ALPHA = 0.15f; // Global double representing the damping factor of the Pagerank algorithm
		
		private int numEdges;
		private long numNodes;
		
		@Override
		public void setup(Configuration conf) {
			numEdges = getEdges().size();
			numNodes = getNumVertices();
		}
		
		@Override
		public void compute(Iterable<DoubleWritable> messages) throws IOException {
			// Calculate new Pagerank
			double pagerankMass = getValue().get() / numEdges;
			if (getSuperstepCount() >= 1) {
				double pagerank = 0.0, lostPagerankMassPart = 0.0, jump = 0.0, link = 0.0;
				
				// Set the lost Pagerank mass from Dangling-nodes
				DoubleWritable sendPagerankMass = getLastAggregatedValue(0); // Aggregator 0 sums up all values send | hence representative to find the total mass lost
				if (sendPagerankMass != null) {
					System.out.printf("sendPagerankMass: %f\n", sendPagerankMass.get()); // TODO remove
					double lostPagerankMass = numNodes - sendPagerankMass.get();
					lostPagerankMassPart = lostPagerankMass / numNodes;
					System.out.printf("lostPagerankMassPart: %f\n", lostPagerankMassPart); // TODO remove
				}
				
				// Sum the Pagerank mass received
				for (DoubleWritable message : messages) {
					pagerank += message.get();
				}
				
				// Finish the Pagerank calculation
				jump = ALPHA / numNodes;
				link = (1 - ALPHA) * (pagerank + lostPagerankMassPart);
				pagerank = jump + link;
				setValue(new DoubleWritable(pagerank));
			}
			
			// Check whether finished and send Pagerank Mass if not
			DoubleWritable lastAverage = getLastAggregatedValue(1); // Aggregator 1 averages all values send compared to the previous round | hence able to estimate convergence
			if (lastAverage != null) { // TODO remove
				System.out.println("AVG Superstep: " + getSuperstepCount() + " | lastAverage: " + lastAverage.get()); // TODO remove
			} // TODO remove
			if (getSuperstepCount() > getMaxIteration()) {
				System.out.printf("More supersteps than iterations\n Superstep: %d Iterations: %d\n", getSuperstepCount(), getMaxIteration()); // TODO remove
				voteToHalt(); // Ran maximal number of iterations specified, hence halt
				return;
			}
			else if (lastAverage != null && getSuperstepCount() > 2 && lastAverage.get() < CONVERGENCE_POINT) {
				System.out.printf("Convergence point reached! Last Average: %f Convergence Point: %f Supersteps: %d\n", lastAverage.get(), CONVERGENCE_POINT, getSuperstepCount()); // TODO remove
				voteToHalt(); // Reached convergence, hence halt
				return;
			}
			sendMessageToNeighbors(new DoubleWritable(pagerankMass));
		}
	}
	
	public static class PageRankTextReader extends VertexInputReader<LongWritable, Text, LongWritable, NullWritable, DoubleWritable> {
		@Override
		public boolean parseVertex(LongWritable key, Text value, Vertex<LongWritable, NullWritable, DoubleWritable> vertex) throws Exception {
			String[] vertices = value.toString().split("\n");
			LongWritable vertexID = new LongWritable(), vertexNeighbor = new LongWritable();
			DoubleWritable vertexValue = new DoubleWritable();
			
			for (String vertice : vertices) {
				String[] vertexInfo = vertice.split("\\s+");
				
				vertexID.set(Long.parseLong(vertexInfo[0])); // Create the node/vertex
				vertex.setVertexID(vertexID);
				vertexValue.set(Double.parseDouble(vertexInfo[1]));
				vertex.setValue(vertexValue);
				for (int j = 2; j < vertexInfo.length; j++) { // Add all the neighbors in the list to the node with their edges
					vertexNeighbor.set(Long.parseLong(vertexInfo[j]));
					vertex.addEdge(new Edge<LongWritable, NullWritable>(vertexNeighbor, null));
				}
			}
			return true;
		}
	}
	
	public GraphJob createJob(HamaConfiguration conf, String inputPath, String outputPath) throws IOException {
		conf.setBoolean("hama.check.missing.vertex", false); // A vertex may be missing (real world), hence check for this should not be performed
		
		GraphJob job = new GraphJob(conf, Pagerank.class); // Main settings
		job.setJobName("PageRank");
		job.setMaxIteration(conf.getInt("iterations", 0));
		job.setInputPath(new Path(inputPath)); // Input settings
		job.setInputFormat(TextInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputPath(new Path(outputPath)); // Output settings
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setVertexClass(PageRankBSP.class); // Vertex settings
		job.setVertexIDClass(LongWritable.class);
		job.setVertexValueClass(DoubleWritable.class);
		job.setEdgeValueClass(NullWritable.class);
		job.setVertexInputReaderClass(PageRankTextReader.class);
		job.setAggregatorClass(SumAggregator.class, AverageAggregator.class); // Aggregator settings
		
		return job;
	}
	
	public int printUsage() {
		System.out.println("usage:\t <input path> <output path> <number of iterations>");
		return -1;
	}
	
	/**
	 * Runs the main program
	 * 
	 * @param args: String array of arguments given at start 
	 * @return -1 in case of error | 0 in case of success
	 * @throws Exception from the iterate() method
	 */
	@Override
	public int run(String[] args) throws Exception {
		int iterations = 0;
		String inputPath = "", outputPath = "";
		HamaConfiguration conf = new HamaConfiguration(getConf());
		
		// Set arguments
		if (args.length < 3) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputPath = args[0];
		outputPath = args[1];
		try {
			iterations = Integer.parseInt(args[2]);
			conf.setInt("iterations", iterations);
		} catch (NumberFormatException e) {
			System.err.println("Error: expected Integer instead of " + args[2]);
			return printUsage();
		}
		
		// Create and start a job
		GraphJob job = createJob(conf, inputPath, outputPath);
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new Pagerank(), args);
		System.exit(result);
	}
}
