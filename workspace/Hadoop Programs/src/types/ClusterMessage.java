package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Object containing a tag value as an integer and a Cluster.
 * Used to communicate in the Hama version of K-Means Clustering
 * @author stevenb
 * @date 14-08-2013
 */
public class ClusterMessage implements WritableComparable<ClusterMessage> {
	
	private String tag;
	private Cluster cluster;
	
	public ClusterMessage(String tag, Cluster cluster) {
		this.tag = tag;
		this.cluster = cluster;
	}
	
	public ClusterMessage() {
		this("", null);
	}
	
	public String getTag() {
		return tag;
	}
	
	public void setTag(String tag) {
		this.tag = tag;
	}
	
	public Cluster getCluster() {
		return cluster;
	}
	
	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}
	
	@Override
	public String toString() {
		return tag + " " + cluster.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(tag);
		cluster.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		tag = in.readLine();
		cluster.readFields(in);
	}
	
	@Override
	public int compareTo(ClusterMessage other) {
		int cmp = tag.compareTo(other.getTag());
		if (cmp != 0) {
			return cmp;
		}
		return cluster.compareTo(other.getCluster());
	}
}
