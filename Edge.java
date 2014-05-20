import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Edge implements Writable {
	
	int source;
	int destination;
	
	//hadoop constructor
	public Edge(){
	}
	public Edge(int s, int d){
		this.source = s;
		this.destination = d;
	}

	public int getSource() {
		return this.source;
	}
	public int getDestination(){
		return this.destination;
	}
	
	public String toString(){
		return "S:" + source+" D:"+ destination;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		source = in.readInt();
		destination = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(destination);
	}

}
