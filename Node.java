import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Node implements  Writable{
	int id;
	double pagerank;
	double oldPagerank;
	int block;
	ArrayList<Integer> blockNeighbors;
	ArrayList<Integer> boundaryNeighbors;

	//Hadoop Constructor
	public Node() {
	}
	public Node(int id, double p, int block){
		this.id = id;
		this.pagerank = p;
		this.oldPagerank = p;
		this.block = block;
		blockNeighbors = new ArrayList<Integer>();
		boundaryNeighbors = new ArrayList<Integer>();
		
	}
	
	public Node(int id, int block){
		this.id = id;
		this.block = block;
		blockNeighbors = new ArrayList<Integer>();
		boundaryNeighbors = new ArrayList<Integer>();
		this.pagerank = (double) (1.0/PageRank.NUM_NODES);
		this.oldPagerank = (double) (1.0/PageRank.NUM_NODES);
	}
	public Node(int id, IntWritable block){
		this.id = id;
		this.block = block.get();
		blockNeighbors = new ArrayList<Integer>();
		boundaryNeighbors = new ArrayList<Integer>();
		this.pagerank = (double) (1.0/PageRank.NUM_NODES);
		this.oldPagerank = (double) (1.0/PageRank.NUM_NODES);
	}
	
	public double getOldPageRank(){
		return this.oldPagerank;
	}
	public double getPageRank(){
		return pagerank;				
	}
	public int getBlockID(){
		return block;
	}
	
	public void setOldPageRank(double pagerank){
		this.oldPagerank = pagerank;
	}
	
	public void setNewPageRank(double pagerank){
		this.pagerank = pagerank;
	}
	
	
	public ArrayList<Integer> getBoundaryNeighbors(){
		return boundaryNeighbors;
	}
	public ArrayList<Integer> getBlockNeighbors(){
		return blockNeighbors;
	}
	
	public void addBlockNeighbor(int n){
		blockNeighbors.add(n);
	}
	public void addBoundaryNeighbor(int n){
		boundaryNeighbors.add(n);
	}
	
	@Override
	public String toString(){
		return id + " " + block + " " + pagerank + " " 
	+ getBlockNeighborsString() + " " + getBoundaryNeighborsString(); 
	}
	
	public int getID() {
		return id;
	}
	
	public int getOrder() {
		int blockDegree;
		int boundaryDegree;
		if(getBlockNeighborsString().equals("-1")){
			blockDegree = 0;
		}
		else{
			blockDegree = this.blockNeighbors.size();
		}
		
		if(getBoundaryNeighborsString().equals("-1")){
			boundaryDegree = 0;
		}
		else{
			boundaryDegree = this.boundaryNeighbors.size();
		}
		return blockDegree + boundaryDegree;
	}
	public String getBlockNeighborsString(){
		StringBuilder buffer = new StringBuilder();
		for(int n : blockNeighbors){
			buffer.append(n+",");
		}
		if (buffer.length() > 0){
			return buffer.toString().substring(0,buffer.length()-1);
		}
		return "-1";
	}
	public String getBoundaryNeighborsString(){
		StringBuilder buffer = new StringBuilder();
		for(int n : boundaryNeighbors){
			buffer.append(n+",");
		}
		if (buffer.length() > 0){	
			return buffer.toString().substring(0,buffer.length()-1);
		}
		return "-1";
	}
	
	public void setBlockNeighbors(String n){
		if (!n.equals("-1")){
			blockNeighbors.clear();
			String [] neighbors = n.split(",");
			for(String id : neighbors){
				blockNeighbors.add(Integer.parseInt(id));
			}
			Collections.sort(blockNeighbors);

		}
	}
	public void setBoundaryNeighbors(String n){
		if (!n.equals("-1")){
			boundaryNeighbors.clear();
			String [] neighbors = n.split(",");
			for(String id : neighbors){
				boundaryNeighbors.add(Integer.parseInt(id));
			}
			Collections.sort(boundaryNeighbors);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		block = in.readInt();
		pagerank = in.readDouble();
		oldPagerank = in.readDouble();
		int next = in.readInt();
		if(boundaryNeighbors != null){
			this.boundaryNeighbors.clear();
		}
		else{
			boundaryNeighbors = new ArrayList<Integer>();
		}
		
		while (next != -2){
			boundaryNeighbors.add(next);
			next = in.readInt();
		}
		if(blockNeighbors != null){
			this.blockNeighbors.clear();
		}
		else{
			blockNeighbors = new ArrayList<Integer>();
		}
		next = in.readInt();
		while (next != -2){
			blockNeighbors.add(next);
			next = in.readInt();
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		out.writeInt(this.block);
		out.writeDouble(this.pagerank);
		out.writeDouble(this.oldPagerank);
		for (Integer n : boundaryNeighbors){
			out.writeInt(n);
		}
		out.writeInt(-2);
		for (Integer n : blockNeighbors){
			out.writeInt(n);
		}
		out.writeInt(-2);
		
	}

}

