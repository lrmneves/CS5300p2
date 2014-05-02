
public class BoundaryCondition {

	Node source;
	Node destination;
	float pagerank;
	
	public BoundaryCondition(Node s, Node d){
		this.source = s;
		this.destination = d;
		this.pagerank = s.pagerank/s.getOrder();
	}
	public BoundaryCondition(Node s, Node d, float pagerank){
		this.source = s;
		this.destination = d;
		this.pagerank = pagerank;
	}
	public Node getSource(){
		return source;
	}
	
	public Node getDestination(){
		return destination;
	}
	
	
}
