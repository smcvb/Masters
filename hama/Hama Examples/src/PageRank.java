import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
//import org.apache.hama.graph.SumAggregator;
//import org.apache.hama.graph.AbsDiffAggregator;
//import org.apache.hama.graph.AverageAggregator;

/**
 * PageRank example
 * Implementation in Apache Hama
 * 
 * To test out how to use Graph framework
 *
 * @author stevenb
 * @version one
 * @date 16-04-2013
 */
public class PageRank {
	
	public static final int TASKS = 5, ITERATIONS = 1000;
	public static final float CONVERGENCE_POINT = 0.0001f;
	
	public static class PageRankBSP extends Vertex<Text, NullWritable, FloatWritable>{
		
		public static final double DAMPING_FACTOR = 0.85;
		
		private int numEdges;
		
		public void setup(Configuration conf) {
			numEdges = this.getEdges().size();
		}
		
		@Override
		public void compute(Iterable<FloatWritable> messages) throws IOException {
			if(this.getSuperstepCount() == 0) { //First round, set base rank
				this.setValue(new FloatWritable(1f / (float) this.getNumVertices()));
			} else if(this.getSuperstepCount() >= 1) {
				float sum = 0.0f;
				for(FloatWritable message : messages)
					sum += message.get();
				float ALPHA = (1f - (float)DAMPING_FACTOR) / (float) this.getNumVertices();
		        this.setValue(new FloatWritable(ALPHA + (float)(0.85 * sum)));
		        //this.setValue(new DoubleWritable(sum));
			}
			
			
			//System.out.printf("getLastAggregatedValue Called");
			FloatWritable lastAverage = getLastAggregatedValue(0);
			if(lastAverage != null)
				System.out.printf("AVG Superstep: %d, lastAverage: %f\n", this.getSuperstepCount(), lastAverage.get());
			if(this.getSuperstepCount() > this.getMaxIteration()){
				System.out.printf("More supersteps than iterations\n Superstep: %d Iterations: %d\n", this.getSuperstepCount(), this.getMaxIteration());
				this.voteToHalt();
				return;
			} else if(lastAverage != null && this.getSuperstepCount() > 2 && lastAverage.get() < CONVERGENCE_POINT) {
				System.out.printf("Convergence point reached! Last Average: %f Convergence Point: %f Supersteps: %d\n", lastAverage.get(), CONVERGENCE_POINT, this.getSuperstepCount());
				this.voteToHalt();
				return;
			}
			this.sendMessageToNeighbors(new FloatWritable(this.getValue().get() / (float) numEdges));
			System.out.printf("COMPUTE Superstep: %d\n", this.getSuperstepCount());
		}
	}
	
	public static class PageRankTextReader extends VertexInputReader<LongWritable, Text, Text, NullWritable, FloatWritable>{
		public boolean parseVertex(LongWritable key, Text value, Vertex<Text, NullWritable, FloatWritable> vertex) throws Exception {
			String list = value.toString();
			String[] vertices = list.split("\n");
			for (String vertice : vertices) {
				String[] vertexInfo = vertice.split("\\s");
				vertex.setVertexID(new Text(vertexInfo[0]));
				for(int j = 1; j < vertexInfo.length; j++){
					vertex.addEdge(new Edge<Text, NullWritable>(new Text(vertexInfo[j]), null));
				}
			}
			return true;
		}
	}
	
	public static GraphJob createJob(HamaConfiguration conf, String[] args) throws IOException{
		GraphJob job = new GraphJob(conf, PageRank.class);
		job.setJobName("PageRank Example");
		
		if(args.length < 2) {
			System.out.println("usage: <input path> <output path> <number of tasks [default is 5]> <iterations [default is 1000]>");
			System.exit(-1);
		}
		
		job.setInputPath(new Path(args[0]));			//Input
		job.setInputFormat(TextInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputPath(new Path(args[1]));			//Output
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setVertexClass(PageRankBSP.class);			//Vertex
		job.setVertexIDClass(Text.class);
		job.setVertexValueClass(FloatWritable.class);
		job.setEdgeValueClass(NullWritable.class);
		job.setVertexInputReaderClass(PageRankTextReader.class);
		job.setAggregatorClass(FloatAverageAggregator.class);//Aggregator
		
		if(args.length >= 4)
			job.setMaxIteration(Integer.parseInt(args[3]));
		if(args.length >= 3)
			job.setNumBspTask(Integer.parseInt(args[2]));
		if(args.length == 2){
			job.setNumBspTask(TASKS);
			job.setMaxIteration(ITERATIONS);
		}
		
		return job;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob job = createJob(conf, args);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}