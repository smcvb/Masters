package types;
import java.util.ArrayList;

public class Node {
	
	private long nodeId;
	private double pagerank;
	private ArrayList<Node> adjacencyList;
	
	public Node(long nodeId, double pagerank, ArrayList<Node> adjacencyList){
		this.nodeId = nodeId;
		this.pagerank = pagerank;
		this.adjacencyList = adjacencyList;
	}
	
	public Node(String nodeString){ //Parses input string to complete node-list
		nodeString = nodeString.replaceAll("[^-a-zA-Z_0-9. \\t]", "").replaceAll("\\s+", " ");
		String[] terms = nodeString.split("\\s");
		if(nodeString.equals("") || terms.length < 2){
			this.nodeId = 0;
			this.pagerank = 0.0;
			this.adjacencyList = new ArrayList<Node>();
		} else {
			this.nodeId = Long.parseLong(terms[0]);
			this.pagerank = Double.parseDouble(terms[1]);
			ArrayList<Node> list = new ArrayList<Node>((terms.length - 2) / 2);
			for(int i = 2; i < terms.length; i += 2){
				Node node = new Node(Long.parseLong(terms[i]), Double.parseDouble(terms[i+1]), new ArrayList<Node>());
				list.add(node);
			}
			this.adjacencyList = list;
		}
	}
	
	public Node(){
		this("");
	}

	public long getNodeId(){	return nodeId; }
	public void setNodeId(int nodeId){ this.nodeId = nodeId; }
	
	public double getPagerank(){ return pagerank; }
	public void setPagerank(double pagerank){ this.pagerank = pagerank; }
	
	public ArrayList<Node> getAdjacencyList(){ return adjacencyList; }
	public void setAdjacencyList(ArrayList<Node> adjacencyList){ this.adjacencyList = adjacencyList; }
	
	public boolean containsList(){
		return !adjacencyList.isEmpty();
	}
	
	public int adjacencyListSize(){
		return adjacencyList.size();
	}
	
	public String toString(){
		return nodeId + " " + pagerank + " " + adjacencyList.toString();
	}
	
	public String structure(){
		return pagerank + " " + adjacencyList.toString();
	}
}
