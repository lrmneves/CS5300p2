import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class NodeOrBCoOrBE implements Writable {
	
	int type;//0 if node, 1 if BCO and 2 if BE 
	Node node;
	BoundaryCondition bco;
	Edge be;
	//Hadoop Constructor
	public NodeOrBCoOrBE(){
		
	}
	public NodeOrBCoOrBE(Node n){
		type = 0;
		node = n;
		bco = null;
		be = null;
	}
	
	public NodeOrBCoOrBE(BoundaryCondition b){
		type = 1;
		node = null;
		bco = b;
		be = null;
	}
	public NodeOrBCoOrBE(Edge e){
		type = 2;
		node = null;
		bco = null;
		be = e;
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
	
	public Edge getBE(){
		return be;
	}
	public void setBE(Edge e){
		this.be = e;
	}
	public int getType() {
		return type;
	}

	public String toString(){
		if (type == 0){
			return node.toString();
		}
		if (type == 1){
			return bco.toString();
		}
		return be.toString();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		if (type == 0){
			node = new Node();
			node.readFields(in);
			bco = null;
			be =null;
		}else if (type == 1){
			bco = new BoundaryCondition();
			bco.readFields(in);
			node = null;
			be = null;
		}else{
			be = new Edge();
			be.readFields(in);
			node = null;
			bco = null;
		}
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		if(this.type == 0){
			node.write(out);
		}
		else if (this.type == 1){
			bco.write(out);
		}
		else{
			be.write(out);
		}
	}
	
}
