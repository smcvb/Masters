package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 * A Writable object to hold a cluster for
 *  the k-means algorithm
 * @author stevenb
 * @date 04-11-2013
 */
public class Cluster implements WritableComparable<Cluster> {
	
	private int index, size, dimensions;
	private Point centroid;
	private Point[] outliers;
	
	public Cluster(int index, int size, int dimensions, Point centroid, Point[] outliers) {
		this.index = index;
		this.size = size;
		this.dimensions = dimensions;
		this.centroid = centroid;
		this.outliers = outliers;
	}
	
	public Cluster(Cluster cluster) {
		index = cluster.getIndex();
		size = cluster.getSize();
		dimensions = cluster.getDimensions();
		centroid = cluster.getCentroid();
		outliers = cluster.getOutliers();
	}
	
	public Cluster() {
		this(-1, 0, 0, null, null);
	}
	
	/**
	 * Retrieve the index number of
	 *  this cluster as an integer.
	 * @return: the index number as an integer.
	 */
	public int getIndex() {
		return index;
	}
	
	/**
	 * Set the index number of 
	 * 	this cluster.
	 * @param index: the new index number
	 *  of this cluster as an integer.
	 */
	public void setIndex(int index) {
		this.index = index;
	}
	
	/**
	 * Retrieve the size of
	 *  this cluster as an integer.
	 * @return: the size as an integer.
	 */
	public int getSize() {
		return size;
	}
	
	/**
	 * Set the size of this cluster.
	 * @param size: the new size
	 *  of this cluster as an integer.
	 */
	public void setSize(int size) {
		this.size = size;
	}
	
	/**
	 * Retrieve the number of dimensions
	 *  this cluster has as an integer.
	 * @return: the number of dimensions as an integer.
	 */
	public int getDimensions() {
		return dimensions;
	}
	
	/**
	 * Set the number of dimensions of 
	 * 	this cluster.
	 * @param dimensions: the new number
	 *  of dimensions of this cluster as an integer.
	 */
	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}
	
	/**
	 * Retrieve the centroid of this cluster
	 *  as a Point object.
	 * @return: the centroid as a Point object.
	 */
	public Point getCentroid() {
		return centroid;
	}
	
	/**
	 * Set the centroid of this cluster.
	 * @param centroid: the new centroid of
	 *  this cluster as a Point object.
	 */
	public void setCentroid(Point centroid) {
		this.centroid = centroid;
	}
	
	/**
	 * Retrieve the outliers of this cluster
	 *  as a Point object array.
	 * @return: the outliers of this cluster
	 *  as a Point object array.
	 */
	public Point[] getOutliers() {
		return outliers;
	}
	
	/**
	 * Set the outliers of this cluster.
	 * @param outliers: the new outliers of 
	 *  this cluster as a Point object array.
	 */
	public void setOutliers(Point[] outliers) {
		this.outliers = Arrays.copyOf(outliers, outliers.length);
	}
	
	/**
	 * Set the cluster its variables
	 *  in one method call.
	 * @param index: the new index number
	 *  of this cluster as an integer.
	 * @param size: the new size
	 *  of this cluster as an integer.
	 * @param dimensions: the new number
	 *  of dimensions of this cluster as an integer.
	 * @param centroid: the new centroid of
	 *  this cluster as a Point object.
	 * @param outliers:the new outliers of 
	 *  this cluster as a Point object array.
	 */
	public void setCluster(int index, int size, int dimensions, Point centroid, Point[] outliers) {
		setIndex(index);
		setSize(size);
		setDimensions(dimensions);
		setCentroid(centroid);
		setOutliers(outliers);
	}
	
	/**
	 * Check whether this is a null pointing
	 *  cluster object.
	 * @return: true if it is a null cluster
	 *  and false if it is not.
	 */
	public boolean isEmpty() {
		if (index == -1) {
			return true;
		}
		return false;
	}
	
	/**
	 * Parse a string which should contain all
	 *  relevant info for a Cluster object as
	 *  a Cluster object.
	 * @param string: a String object containing the
	 *  information of a complete cluster. 
	 * @param kmeans: the number of clusters k this
	 *  cluster object will be part of; needed for
	 *  the number of outliers.
	 */
	public void parseCluster(String string, int kmeans) {
		String clusterInfo = string.replaceAll("[\\[\\]\\{\\}]", "");
		String[] clusterInfoValues = clusterInfo.toString().replaceAll("\\s+", " ").split("\\s+");
		int start = 0, end = clusterInfoValues.length;
		outliers = new Point[kmeans - 1];
		
		// Parse index and size
		try {
			index = (int) Double.parseDouble(clusterInfoValues[0]); // Use parseDouble() to make '0' parsing valid
			size = (int) Double.parseDouble(clusterInfoValues[1]);
			dimensions = (int) Double.parseDouble(clusterInfoValues[2]);
		} catch (NumberFormatException e) {
			System.err.println("Error: expected an Integer instead of " + clusterInfoValues[0] + " " + clusterInfoValues[1]);
		}
		
		// Empty cluster encountered, cannot parse mean and outliers
		if (isEmpty()) {
			return;
		}
		
		// Parse cluster centroid
		start += 3;
		double[] coordinates = new double[dimensions];
		for (int i = start; i < start + dimensions; i++) {
			try {
				coordinates[i - 3] = Double.parseDouble(clusterInfoValues[i]);
			} catch (NumberFormatException e) {
				System.err.println("Error in centroid: expected a Double instead of " + clusterInfoValues[i] + " at index " + i);
			}
		}
		centroid = new Point(coordinates, new String[0]);
		
		// Parse clusters k - 1 outliers
		for (int i = 0; i < kmeans - 1; i++) {
			double[] outlierCoordinates = new double[dimensions];
			start += dimensions;
			if (start == end) {
				break;
			}
			for (int j = start; j < start + dimensions; j++) {
				try {
					outlierCoordinates[j - start] = Double.parseDouble(clusterInfoValues[j]);
				} catch (NumberFormatException e) {
					System.err.println("Error in outlier: expected a Double instead of " + clusterInfoValues[i] + " at index " + i);
				}
			}
			outliers[i] = new Point(outlierCoordinates, new String[0]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		
		b.append(index);
		b.append(" ");
		b.append(size);
		b.append(" ");
		b.append(dimensions);
		
		if (centroid != null) {
			b.append("[");
			b.append(centroid.toString());
			b.append(" ] { ");
		}
		
		if (outliers != null) {
			for (int i = 0; i < outliers.length; i++) {
				if (outliers[i] != null) {
					b.append("[");
					b.append(outliers[i].toString());
					b.append(" ] ");
				} else {
					b.append("[ ] ");
				}
			}
		}
		b.append(" }");
		
		return b.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		index = in.readInt();
		size = in.readInt();
		dimensions = in.readInt();
		centroid = new Point();
		centroid.readFields(in);
		int numberOfOutliers = in.readInt();
		outliers = new Point[numberOfOutliers];
		for (int i = 0; i < numberOfOutliers; i++) {
			outliers[i] = new Point();
			outliers[i].readFields(in);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(index);
		out.writeInt(size);
		out.writeInt(dimensions);
		centroid.write(out);
		int numberOfOutliers = outliers.length;
		out.writeInt(numberOfOutliers);
		for (int i = 0; i < numberOfOutliers; i++) {
			outliers[i].write(out);
		}
	}
	
	@Override
	public int compareTo(Cluster other) {
		int cmp = index - other.getIndex();
		if (cmp != 0) {
			return cmp;
		}
		cmp = size - other.getSize();
		if (cmp != 0) {
			return cmp;
		}
		cmp = dimensions - other.getDimensions();
		if (cmp != 0) {
			return cmp;
		}
		
		cmp = centroid.compareTo(other.getCentroid());
		if (cmp != 0) {
			return cmp;
		}
		
		Point[] otherOutliers = other.getOutliers();
		for (int i = 0; i < outliers.length; i++) {
			try {
				cmp = outliers[i].compareTo(otherOutliers[i]);
				if (cmp != 0) {
					return cmp;
				}
			} catch (Exception e) {
				return -1;
			}
		}
		
		return 0;
	}
}
