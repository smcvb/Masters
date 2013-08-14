package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Node> {
	
	private BooleanWritable complete, structure, mass, hasFilledList;
	private LongWritable nodeId;
	private DoubleWritable pagerank;
	private LongArrayWritable adjacencyList;
	
	public Node() {
		complete = new BooleanWritable(false);
		structure = new BooleanWritable(false);
		mass = new BooleanWritable(false);
		hasFilledList = new BooleanWritable(false);
		nodeId = new LongWritable(0);
		pagerank = new DoubleWritable(0.0f);
		adjacencyList = null;
	}
	
	public LongWritable getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(long nodeId) {
		this.nodeId = new LongWritable(nodeId);
	}
	
	public void setNodeId(LongWritable nodeId) {
		this.nodeId = nodeId;
	}
	
	public DoubleWritable getPagerank() {
		return pagerank;
	}
	
	public void setPagerank(double pagerank) {
		this.pagerank = new DoubleWritable(pagerank);
	}
	
	public void setPagerank(DoubleWritable pagerank) {
		this.pagerank = pagerank;
	}
	
	public LongArrayWritable getAdjacencyList() {
		return adjacencyList;
	}
	
	public void setAdjacencyList(LongArrayWritable adjacencyList) {
		if (adjacencyList != null) {
			hasFilledList.set(true);
			this.adjacencyList = adjacencyList;
		}
	}
	
	public void setAdjacencyList(LongWritable[] adjacencyArray) {
		if (adjacencyArray.length > 0) {
			hasFilledList.set(true);
			LongArrayWritable linksWritable = new LongArrayWritable(LongWritable.class);
			linksWritable.set(adjacencyArray);
			adjacencyList = linksWritable;
		}
	}
	
	public void setAsCompleteNode() {
		complete.set(true);
	}
	
	public boolean isCompleteNode() {
		return complete.get();
	}
	
	public void setAsStructureNode() {
		structure.set(true);
	}
	
	public boolean isStructureNode() {
		return structure.get();
	}
	
	public void setAsMassNode() {
		mass.set(true);
	}
	
	public boolean isMassNode() {
		return mass.get();
	}
	
	public boolean hasStructure() {
		return hasFilledList.get();
	}
	
	public String structure() {
		if (hasFilledList.get()) {
			return pagerank.toString() + "\t" + adjacencyList.toString();
		}
		else {
			return pagerank.toString();
		}
	}
	
	@Override
	public String toString() {
		if (hasFilledList.get()) {
			return nodeId.toString() + "\t" + pagerank.toString() + "\t" + adjacencyList.toString();
		}
		else {
			return nodeId.toString() + "\t" + pagerank.toString();
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		complete.readFields(in);
		structure.readFields(in);
		mass.readFields(in);
		hasFilledList.readFields(in);
		nodeId.readFields(in);
		pagerank.readFields(in);
		//TODO System.out.printf("STRUCT: %b NODEID: %d PAGERANK: %f\n", structure.get(), nodeId.get(), pagerank.get());
		if (hasFilledList.get()) {
			adjacencyList = new LongArrayWritable(LongWritable.class);
			adjacencyList.readFields(in);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		complete.write(out);
		structure.write(out);
		mass.write(out);
		hasFilledList.write(out);
		nodeId.write(out);
		pagerank.write(out);
		if (hasFilledList.get()) {
			adjacencyList.write(out);
		}
	}
	
	@Override
	public int hashCode() {
		int result = nodeId != null ? nodeId.hashCode() : 0;
		result = 163 * result + (pagerank != null ? pagerank.hashCode() : 0);
		result = 163 * result + (adjacencyList != null ? adjacencyList.hashCode() : 0);
		return result;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Node n = (Node) o;
		if (nodeId != null ? !nodeId.equals(n.getNodeId()) : n.getNodeId() != null) {
			return false;
		}
		if (pagerank != null ? !pagerank.equals(n.getPagerank()) : n.getPagerank() != null) {
			return false;
		}
		if (adjacencyList != null ? !adjacencyList.equals(n.getAdjacencyList()) : n.getAdjacencyList() != null) {
			return false;
		}
		return true;
	}
	
	@Override
	public int compareTo(Node n) {
		int cmp = nodeId.compareTo(n.getNodeId());
		if (cmp != 0) {
			return cmp;
		}
		cmp = pagerank.compareTo(n.getPagerank());
		if (cmp != 0) {
			return cmp;
		}
		cmp += adjacencyList.equals(n.getAdjacencyList()) ? 0 : -1;
		return cmp;
	}
}
