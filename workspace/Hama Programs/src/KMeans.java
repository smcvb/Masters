import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

import types.Cluster;
import types.ClusterMessage;
import types.Point;

/**
 * Hama program to run the k-means algorithm
 * @author stevenb
 * @date 02-09-2013
 */
public class KMeans extends Configured implements Tool {
	
	public static final float CONVERGENCE_POINT = 0.01f;
	public static final String POINT = "POINT";
	
	public static class KMeansBSP extends BSP<LongWritable, Text, IntWritable, Text, ClusterMessage> {
		
		private int kmeans, round, iterations;
		private Cluster me;
		
		@Override
		public void setup(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer) throws IOException { //initialize
			kmeans = peer.getNumPeers();
			round = 0;
			iterations = peer.getConfiguration().getInt("iterations", 0);
			me = new Cluster(peer.getPeerIndex(), 0, 0, new Point(), new Point[kmeans - 1]);
		}
		
		@Override
		/**
		 * The KMeans Clustering algorithm
		 */
		public void bsp(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer) throws IOException, InterruptedException, SyncException {
			final ArrayList<Point> points = new ArrayList<Point>();
			final HashMap<String, Cluster> clusters = new HashMap<String, Cluster>(kmeans);
			boolean converged = false;
			
			initialize(peer);
			peer.sync();
			while (!converged && round < iterations) {
				if (round != 0) { // The first round accounts for initialization, hence first needs to calculate the mean.
					assignPoints(peer, points, clusters);
					peer.sync();
				}
				receiveMessages(peer, points, clusters); // Receive the new points
				updateCluster(peer, points); // Update this cluster its info according to the new points
				peer.sync();
				converged = receiveMessages(peer, points, clusters); // Set the new clusters and check if converged
				round++;
			}
			
			writeOutput(peer, points, clusters, converged);
		}
		
		/**
		 * The first round of KMeans, hence first need to initialize
		 *  all the point to a cluster.
		 * Points are assigned randomly to a cluster (Random Partitioning)
		 *  within this program.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @throws IOException from the readNext() and send() methods.
		 */
		private void initialize(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer) throws IOException {
			System.out.println(peer.getPeerName() + " | initialize |"); // TODO REMOVE
			LongWritable key = new LongWritable();
			Text value = new Text();
			String[] lines = null;
			Random random = new Random();
			
			while (peer.readNext(key, value)) {
				lines = value.toString().split("\n");
				for (int i = 0; i < lines.length; i++) {
					int clusterIndex = random.nextInt(kmeans);
					Cluster point = new Cluster(-1, -1, -1, new Point(lines[i]), new Point[0]);
					ClusterMessage m = new ClusterMessage(POINT, point);
					String name = peer.getPeerName(clusterIndex);
					peer.send(name, m);
				}
			}
		}
		
		/**
		 * Receive all the messages send by other peers at this peer.
		 * Depending on the tag contained in the ClusterMessage object,
		 *  one can see whether it is a new point or a cluster centroid
		 *  message.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @param clusters: a HashMap object containing all current clusters.
		 * @return true or false depending on whether the algorithm converged.
		 * @throws IOException from the getCurrentMessage() method.
		 */
		private boolean receiveMessages(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points, HashMap<String, Cluster> clusters) throws IOException {
			System.out.println(peer.getPeerName() + " | receiveMessages |"); // TODO REMOVE
			boolean converged = false;
			ClusterMessage message;
			HashMap<String, Cluster> newClusters = new HashMap<String, Cluster>(kmeans);
			
			while ((message = peer.getCurrentMessage()) != null) {
				String tag = message.getTag();
				if (tag.equals(POINT)) { // Received a Point message
					points.add(message.getCluster().getCentroid());
				} else { // Probably a Cluster mean
					newClusters.put(tag, message.getCluster());
				}
			}
			
			if (newClusters.size() > 0) { // Only if any clusters are received, can we check for convergence and set the new clusters
				converged = checkConvergence(clusters, newClusters);
				setNewClusters(peer.getPeerName(), clusters, newClusters);
			}
			return converged;
		}
		
		/**
		 * Check whether a point contained in this cluster is 
		 *  closer to another clusters its mean. If so, assign 
		 *  that point to that clusters and remove it from your own.
		 * The points are send directly, because storing them could
		 *  form a memory bottleneck
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @param clusters: a HashMap object containing all current clusters.
		 * @throws IOException from the send() method
		 */
		private void assignPoints(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points, HashMap<String, Cluster> clusters) throws IOException {
			System.out.println(peer.getPeerName() + " | assignPoints |"); // TODO REMOVE
			Iterator<Point> pointsIterator = points.iterator();
			
			while (pointsIterator.hasNext()) {
				double dist = 0.0, minDist = Double.MAX_VALUE;
				String name = "";
				final Point currentPoint = pointsIterator.next();
				
				for (Entry<String, Cluster> peerInfo : clusters.entrySet()) {
					Point centroid = peerInfo.getValue().getCentroid();
					dist = currentPoint.calculateDistance(centroid);
					
					if (dist < minDist) {
						minDist = dist;
						name = peerInfo.getKey();
					}
				}
				
				if (!name.equals(peer.getPeerName())) { // If the point did not stay in my cluster, send a message to the correct peer
					pointsIterator.remove();
					ClusterMessage m = new ClusterMessage(POINT, new Cluster(-1, -1, -1, currentPoint, new Point[0]));
					peer.send(name, m);
				}
			}
		}
		
		/**
		 * Have assigned points and received new points,
		 *  hence ready to update the mean, its outliers 
		 *  and report this to the other peers.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @throws IOException from recalculateMean(), setOutliers() and
		 *  broadcastClusterInfo() methods.
		 */
		private void updateCluster(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points) throws IOException {
			System.out.println(peer.getPeerName() + " | updateCluster |"); // TODO REMOVE
			recalculateMean(peer, points); // set the new centroid of this peer
			setOutliers(peer, points); // set the new outliers of this peer
			broadcastClusterInfo(peer); // Send your cluster info around
		}
		
		/**
		 * Recalculate the centroid of this cluster.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @throws IOException from the broadcastClusterInfo() method
		 */
		private void recalculateMean(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points) throws IOException {
			System.out.println(peer.getPeerName() + " | recalculateMean |"); // TODO REMOVE
			int size = 0;
			Point newCentroid = new Point();
			
			for (Point point : points) {
				if (size == 0) {
					newCentroid.setCoordinates(point.getCoordinates());
					me.setDimensions(newCentroid.getCoordinates().length); // set the number of Dimensions
				} else {
					newCentroid.add(point);
				}
				size++;
			}
			newCentroid.divide(size);
			me.setSize(size); // set the size
			me.setCentroid(newCentroid);
		}
		
		/**
		 * Set the k - 1 outliers of this cluster, by 
		 *  calculating which points have the largest
		 *  distance to the center 
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @throws IOException from the broadcastOutliers() method.
		 */
		private void setOutliers(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points) throws IOException {
			System.out.println(peer.getPeerName() + " | setOutliers |"); // TODO REMOVE
			Point centroid = me.getCentroid();
			Point[] outliers = new Point[kmeans - 1];
			Point[] newOutliers = new Point[outliers.length];
			
			for (Point point : points) {
				boolean gotPosition = false;
				double dist = point.calculateDistance(centroid);
				for (int k = 0; k < outliers.length; k++) {
					if (!gotPosition) {
						newOutliers[k] = outliers[k];
						if (newOutliers[k] == null || outliers[k].calculateDistance(centroid) < dist) {
							newOutliers[k] = new Point(point.getCoordinates(), new String[0]);
							gotPosition = true;
						}
					} else {
						newOutliers[k] = outliers[k - 1];
					}
				}
				outliers = Arrays.copyOf(newOutliers, newOutliers.length);
			}
			me.setOutliers(newOutliers);
		}
		
		/**
		 * Broadcast the newly calculated centroid of this cluster
		 *  to the other clusters.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @throws IOException from the send() method
		 */
		private void broadcastClusterInfo(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer) throws IOException {
			System.out.println(peer.getPeerName() + " | broadcastClusterInfo | sending " + me.toString()); // TODO REMOVE
			String[] clusterNames = peer.getAllPeerNames();
			for (String clusterName : clusterNames) {
				final ClusterMessage m = new ClusterMessage(peer.getPeerName(), new Cluster(me));
				peer.send(clusterName, m);
			}
		}
		
		/**
		 * Check whether the algorithm has converged, 
		 *  compared to the previous cluster centroid and
		 *  the current cluster centroids.
		 * @param clusters: a HashMap object containing all current clusters.
		 * @param newClusters: a HashMap containing the new clusters.
		 * @return true in case the algorithm, false
		 *  in case it did not.
		 */
		private boolean checkConvergence(HashMap<String, Cluster> clusters, HashMap<String, Cluster> newClusters) {
			System.out.println("| checkConvergence |"); // TODO REMOVE
			if (clusters.size() != newClusters.size()) { // if the sizes do not equal, 'clusters' was smaller than kmeans the previous round; hence no convergence
				return false;
			}
			
			for (Entry<String, Cluster> entry : clusters.entrySet()) {
				Cluster oldCluster = entry.getValue();
				Cluster newCluster = newClusters.get(entry.getKey());
				if (oldCluster.getSize() == 0 || newCluster.getSize() == 0) { // Found a cluster with no points, hence no mean to compare
					return false; // Thus cannot check for convergence
				} else if (oldCluster.getCentroid().compareTo(newCluster.getCentroid(), CONVERGENCE_POINT) != 0) {
					return false;
				}
			}
			return true;
		}
		
		/**
		 * Set the newly found clusters as the current clusters
		 * @param clusters: a HashMap object containing all current clusters.
		 * @param newClusters: a HashMap containing the new clusters.
		 */
		private void setNewClusters(String peerName, HashMap<String, Cluster> clusters, HashMap<String, Cluster> newClusters) {
			System.out.println("| setNewClusters |"); // TODO REMOVE
			int emptyClusters = 0;
			
			for (Entry<String, Cluster> entry : newClusters.entrySet()) {
				Cluster newCluster = entry.getValue();
				if (newCluster.getSize() <= 0) {
					emptyClusters++;
				}
				clusters.put(entry.getKey(), newCluster);
			}
			
			if (emptyClusters > 0) {
				selectNewClusters(peerName, clusters, emptyClusters);
			}
		}
		
		/**
		 * Select outliers from the largest cluster to become new clusters
		 *  in place of the empty clusters indexes.
		 * This method will first select on which indexes empty clusters were 
		 *  found and then pick from the cluster with the most points the 
		 *  outliers to become new cluster centroids.
		 * @param peerName: a String variable containing the name of this peer
		 * @param clusters: a HashMap object containing all current clusters.
		 * @param numEmptyClusters: an Integer variable storing the number of
		 *  empty clusters found and thus empty cluster indexes to look for.
		 */
		private void selectNewClusters(String peerName, HashMap<String, Cluster> clusters, int numEmptyClusters) {
			System.out.println("| selectNewClusters |"); // TODO REMOVE
			int i = 0;
			String[] emptyClusterNames = new String[numEmptyClusters];
			Cluster largestCluster = new Cluster();
			
			for (Entry<String, Cluster> entry : clusters.entrySet()) {
				if (entry.getValue().getSize() == 0) { // Find the indexes of the empty clusters
					emptyClusterNames[i] = entry.getKey();
					i++;
				} // Select the largest cluster to retrieve the outliers from to become new clusters 
				else if (entry.getValue().getSize() > largestCluster.getSize()) {
					largestCluster = new Cluster(entry.getValue());
				}
			}
			
			// Select the new cluster centroids
			Point[] outliers = largestCluster.getOutliers();
			for (int j = 0; j < numEmptyClusters; j++) {
				Cluster newCluster = new Cluster(clusters.get(emptyClusterNames[j]));
				newCluster.setCluster(newCluster.getIndex(), 1, largestCluster.getDimensions(), outliers[j], new Point[0]);
				clusters.remove(emptyClusterNames[j]); // Setting the new key-value pair
				clusters.put(emptyClusterNames[j], newCluster);
				if (emptyClusterNames[j].equals(peerName)) { // If my cluster was empty and was replaced by this one..
					me.setSize(1);
					me.setCentroid(new Point(outliers[j]));
				}
			}
		}
		
		/**
		 * Final method to give information concerning the execution times and clusters found
		 * All the points belonging to this peer will be written to file.
		 * @param peer: a BSPPeer object containing all the information about 
		 * 	this BSPPeer task.
		 * @param points: an ArrayList containing all the points belonging to this peer.
		 * @param clusters: a HashMap object containing all current clusters.
		 * @param converged: a boolean variable which is true or false,
		 * 	depending on whether the algorithm converged yes or no.
		 * @throws IOException from the write method.
		 */
		public void writeOutput(BSPPeer<LongWritable, Text, IntWritable, Text, ClusterMessage> peer, ArrayList<Point> points, HashMap<String, Cluster> clusters, boolean converged) throws IOException { // Close
			System.out.println(peer.getPeerName() + " | writeOutput |"); // TODO REMOVE
			if (peer.getPeerIndex() == 0 && converged) {
				System.out.printf("\n\nClusters Converged in Iteration %d\n\n", round);
				for (Entry<String, Cluster> entry : clusters.entrySet()) {
					System.out.printf("Cluster:\t%s\n", entry.getValue().toString());
				}
			} else if (peer.getPeerIndex() == 0) {
				System.out.printf("\n\nClusters did not converge, but reached the maximum number of iterations\n\n");
				for (Entry<String, Cluster> entry : clusters.entrySet()) {
					System.out.printf("Cluster:\t%s\n", entry.getValue().toString());
				}
			}
			
			for (Point point : points) { // Write the mean - Point pairs out to a file
				peer.write(new IntWritable(me.getIndex()), new Text(point.toString()));
			}
			points.clear(); // Empty memory
		}
	}
	
	/**
	 * Create the job.
	 * @param args: String array of arguments
	 * @param conf: a HamaConfiguration Object for the BSP job
	 * @return a finalized BSPJob Object for this BSP job
	 * @throws IOException for creating the BSP job Object
	 */
	public static BSPJob createJob(HamaConfiguration conf, Path inputPath, Path outputPath, int kmeans) throws IOException {
		BSPJob job = new BSPJob(conf, KMeans.class); // Main settings
		job.setJobName("KMeans Clustering");
		job.setBspClass(KMeansBSP.class);
		job.setNumBspTask(kmeans);
		job.setInputPath(inputPath); // Input settings
		job.setInputFormat(TextInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputPath(outputPath); // Output settings
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}
	
	/**
	 * Prints out the usages of this program in case the user
	 *  gave incorrect input
	 * @param numArgs: number of arguments in the String array object
	 */
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> <k mean points> <number of iterations>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	/**
	 * Runs the main program
	 * 
	 * @param args: String array of arguments given at start 
	 * @return -1 in case of error | 0 in case of success
	 * @throws Exception from the createJob() and the waitForCompletion() methods
	 */
	public int run(String[] args) throws Exception {
		int kmeans = 0, iterations = 0;
		Path inputPath = null, outputPath = null;
		HamaConfiguration conf = new HamaConfiguration(getConf());
		
		// Set arguments
		if (args.length < 4) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		try {
			kmeans = Integer.parseInt(args[2]);
			conf.setInt("kmeans", kmeans);
			iterations = Integer.parseInt(args[3]);
			conf.setInt("iterations", iterations);
		} catch (NumberFormatException e) {
			System.err.println("Error: expected Integers instead of " + args[2] + " (arg 2) and " + args[3] + " (arg 3)");
			return printUsage();
		}
		
		// Create and start a job
		BSPJob job = createJob(conf, inputPath, outputPath, kmeans);
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new KMeans(), args);
		System.exit(result);
	}
}
