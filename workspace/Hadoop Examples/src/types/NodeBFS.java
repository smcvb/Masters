package types;

import java.util.ArrayList;

/**
 * Type class for a graph node in an 
 *  adjacency list
 *  
 * @author stevenb
 * @date 03-04-2013
 */
public class NodeBFS {
	
	private int nodeId,
			distance;
	private ArrayList<NodeBFS> adjacencyList;
	
	public NodeBFS(int nodeId, int distance, ArrayList<NodeBFS> adjacencyList) {
		this.nodeId = nodeId;
		this.distance = distance;
		this.adjacencyList = adjacencyList;
	}
	
	public NodeBFS(String nodeString) { //Parses input string to complete node-list
		nodeString = nodeString.replaceAll("[^-a-zA-Z_0-9 \\t]", "").replaceAll("\\s+", " ");
		String[] terms = nodeString.split("\\s");
		if (nodeString.equals("") || terms.length < 2) {
			nodeId = 0;
			distance = 0;
			adjacencyList = new ArrayList<NodeBFS>();
		} else {
			nodeId = Integer.parseInt(terms[0]);
			distance = Integer.parseInt(terms[1]);
			ArrayList<NodeBFS> list = new ArrayList<NodeBFS>((terms.length - 2) / 2);
			for (int i = 2; i < terms.length; i += 2) {
				NodeBFS node = new NodeBFS(Integer.parseInt(terms[i]), Integer.parseInt(terms[i + 1]), new ArrayList<NodeBFS>());
				list.add(node);
			}
			adjacencyList = list;
		}
	}
	
	public NodeBFS() {
		this("");
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	
	public int getDistance() {
		return distance;
	}
	
	public void setDistance(int distance) {
		this.distance = distance;
	}
	
	public ArrayList<NodeBFS> getAdjacencyList() {
		return adjacencyList;
	}
	
	public void setAdjacencyList(ArrayList<NodeBFS> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public boolean containsList() {
		return !adjacencyList.isEmpty();
	}
	
	@Override
	public String toString() {
		return nodeId + " " + distance + " " + adjacencyList.toString();
	}
	
	public String structure() {
		return distance + " " + adjacencyList.toString();
	}
}
