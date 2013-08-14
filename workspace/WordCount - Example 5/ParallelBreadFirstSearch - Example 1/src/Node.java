import java.util.ArrayList;

/**
 * Type class for a graph node in an 
 *  adjacency list
 *  
 * @author stevenb
 * @date 03-04-2013
 */
public class Node {
	
	private int nodeId,
				distance;
	private ArrayList<Node> adjacencyList;
	
	public Node(int nodeId, int distance, ArrayList<Node> adjacencyList){
		this.nodeId = nodeId;
		this.distance = distance;
		this.adjacencyList = adjacencyList;
	}
	
	public Node(String nodeString){ //Parses input string to complete node-list
		nodeString = nodeString.replaceAll("[^-a-zA-Z_0-9 \\t]", "").replaceAll("\\s+", " ");
		String[] terms = nodeString.split("\\s");
		if(nodeString.equals("") || terms.length < 2){
			this.nodeId = 0;
			this.distance = 0;
			this.adjacencyList = new ArrayList<Node>();
		} else {
			this.nodeId = Integer.parseInt(terms[0]);
			this.distance = Integer.parseInt(terms[1]);
			ArrayList<Node> list = new ArrayList<Node>((terms.length - 2) / 2);
			for(int i = 2; i < terms.length; i += 2){
				Node node = new Node(Integer.parseInt(terms[i]), Integer.parseInt(terms[i+1]), new ArrayList<Node>());
				list.add(node);
			}
			this.adjacencyList = list;
		}
	}
	
	public Node(){
		this("");
	}

	public int getNodeId(){	return nodeId; }
	public void setNodeId(int nodeId){ this.nodeId = nodeId; }
	
	public int getDistance(){ return distance; }
	public void setDistance(int distance){ this.distance = distance; }
	
	public ArrayList<Node> getAdjacencyList(){ return adjacencyList; }
	public void setAdjacencyList(ArrayList<Node> adjacencyList){ this.adjacencyList = adjacencyList; }
	
	public boolean containsList(){
		return !adjacencyList.isEmpty();
	}
	
	public String toString(){
		return nodeId + " " + distance + " " + adjacencyList.toString();
	}
	
	public String structure(){
		return distance + " " + adjacencyList.toString();
	}
}
