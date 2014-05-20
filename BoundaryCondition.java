import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class BoundaryCondition implements Writable{

	int source;
	int destination;
	double pagerank;
	//hadoop constructor
	public BoundaryCondition(){
	}
	
	public BoundaryCondition(int s, int d){
		this.source = s;
		this.destination = d;
		this.pagerank = 0.0f;
	}
	public BoundaryCondition(int s, int d, double e){
		this.source = s;
		this.destination = d;
		this.pagerank = e;
	}
	public int getSource(){
		return source;
	}
	
	public int getDestination(){
		return destination;
	}
	public double getPageRank() {
		return pagerank;
	}
	public String toString(){
		return "S:" + source +" D:"+ destination +" PR:"+ pagerank;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		source = in.readInt();
		destination = in.readInt();
		pagerank = in.readDouble();		
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(destination);
		out.writeDouble(pagerank);
	}
}
