import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;

public class Node{
	int id;
	float pagerank;
	float oldPagerank;
	IntWritable block;
	ArrayList<Node> blockNeighbors;
	ArrayList<Node> boundaryNeighbors;
	
	public Node(int id, float w, IntWritable block){
		this.id = id;
		this.pagerank = w;
		this.block = block;
		blockNeighbors = new ArrayList<Node>();
		boundaryNeighbors = new ArrayList<Node>();
	}
	
	public Node(int id, IntWritable block){
		this.id = id;
		this.block = block;
		blockNeighbors = new ArrayList<Node>();
		boundaryNeighbors = new ArrayList<Node>();
	}
	
	public Node(int id, float w){
		this.id = id;
		this.pagerank = w;
		blockNeighbors = new ArrayList<Node>();
		boundaryNeighbors = new ArrayList<Node>();
	}
	
	public float getOldPageRank(){
		return this.oldPagerank;
	}
	public float getPageRank(){
		return pagerank;				
	}
	public IntWritable getBlockID(){
		return block;
	}
	
	public void setOldPageRank(float pagerank){
		this.oldPagerank = pagerank;
	}
	
	public void setNewPageRank(float pagerank){
		this.pagerank = pagerank;
	}
	
	public void addBlockNeighbor(Node n){
		blockNeighbors.add(n);
	}
	public ArrayList<Node> getBoundaryNeighbors(){
		return boundaryNeighbors;
	}
	public ArrayList<Node> getBlockNeighbors(){
		return blockNeighbors;
	}
	
	public void addBoundaryNeighbor(Node n){
		boundaryNeighbors.add(n);
	}
	
	@Override
	public String toString(){
		return "Node: "+ id + " Block: " + block + " Pagerank: " + pagerank; 
	}
	
	public int getID() {
		return id;
	}
	
	public float getOrder() {
		return this.blockNeighbors.size() + this.boundaryNeighbors.size();
	}
}

