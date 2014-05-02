
public class Edge {
	
	Node source;
	Node destination;
	
	public Edge(Node s, Node d){
		this.source = s;
		this.destination = d;
	}

	public Node getSource() {
		return this.source;
	}
	public Node getDestination(){
		return this.destination;
	}

}
