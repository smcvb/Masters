package types;

import java.util.ArrayList;

/**
 * Type class for a graph node in an 
 *  adjacency list with pagerank
 *  
 * @author stevenb
 * @date 03-04-2013
 */
public class NodePR {
	
	private int nodeId;
	private double pagerank;
	private ArrayList<NodePR> adjacencyList;
	
	public NodePR(int nodeId, double pagerank, ArrayList<NodePR> adjacencyList) {
		this.nodeId = nodeId;
		this.pagerank = pagerank;
		this.adjacencyList = adjacencyList;
	}
	
	public NodePR(String nodeString) { //Parses input string to complete node-list
		nodeString = nodeString.replaceAll("[^-a-zA-Z_0-9. \\t]", "").replaceAll("\\s+", " ");
		String[] terms = nodeString.split("\\s");
		if (nodeString.equals("") || terms.length < 2) {
			nodeId = 0;
			pagerank = 0.0;
			adjacencyList = new ArrayList<NodePR>();
		} else {
			nodeId = Integer.parseInt(terms[0]);
			pagerank = Double.parseDouble(terms[1]);
			ArrayList<NodePR> list = new ArrayList<NodePR>((terms.length - 2) / 2);
			for (int i = 2; i < terms.length; i += 2) {
				NodePR node = new NodePR(Integer.parseInt(terms[i]), Double.parseDouble(terms[i + 1]), new ArrayList<NodePR>());
				list.add(node);
			}
			adjacencyList = list;
		}
	}
	
	public NodePR() {
		this("");
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	
	public double getPagerank() {
		return pagerank;
	}
	
	public void setPagerank(double pagerank) {
		this.pagerank = pagerank;
	}
	
	public ArrayList<NodePR> getAdjacencyList() {
		return adjacencyList;
	}
	
	public void setAdjacencyList(ArrayList<NodePR> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public boolean containsList() {
		return !adjacencyList.isEmpty();
	}
	
	public int adjacencyListSize() {
		return adjacencyList.size();
	}
	
	@Override
	public String toString() {
		return nodeId + " " + pagerank + " " + adjacencyList.toString();
	}
	
	public String structure() {
		return pagerank + " " + adjacencyList.toString();
	}
}
