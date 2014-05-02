
public class NodeOrBCo {
	
	boolean isNode;
	Node node;
	BoundaryCondition bco;
	
	public NodeOrBCo(Node n){
		isNode = true;
		node = n;
		bco = null;
	}
	
	public NodeOrBCo(BoundaryCondition b){
		isNode = false;
		node = null;
		bco = b;
	}
	
	public Node getNode(){
		return node;
	}
	
	public void setNode(Node n){
		this.node = n;
	}
	public BoundaryCondition getBCo(){
		return bco;
	}
	public void setBCo(BoundaryCondition bco){
		this.bco = bco;
	}

	public boolean isNode() {
		return isNode;
	}
	
}
