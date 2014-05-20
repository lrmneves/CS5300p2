import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.*;

// main reducer task
public class BlockReducer extends Reducer<IntWritable,NodeOrBCoOrBE, IntWritable, Node>{
	ArrayList<Edge> BE; 
	ArrayList<BoundaryCondition> BCO;
	HashMap<Integer,Node> nodeValues; //hashmap to access node values by id
	HashMap<Integer,Double> initialPR;//hashmap to save the initial pagerank to be compared
	HashMap<Integer,ArrayList<Integer>> BENodes;
	HashMap<Integer,ArrayList<BoundaryCondition>> BCoNodes;
	int max_iteration = 20;  
	
	
	public void reduce(IntWritable id, Iterable<NodeOrBCoOrBE> nodeList,Context context) throws IOException, InterruptedException
	{	
		//BE = new ArrayList<Edge>();
		//BCO = new ArrayList<BoundaryCondition>();
		nodeValues = new HashMap<Integer,Node>();
		initialPR = new HashMap<Integer,Double>();
		BENodes = new HashMap<Integer,ArrayList<Integer>>();
		BCoNodes = new HashMap<Integer,ArrayList<BoundaryCondition>>();
		Node node = null;
		int max = -1;//store the largest numbered node in the block
		//int blockMax = PageRank.returnBlockLimit(id.get());
		
		// iterate through the values
		for (Iterator<NodeOrBCoOrBE> i = nodeList.iterator();i.hasNext();)
		{	
			NodeOrBCoOrBE n = i.next();
			
			// if n is the node
			if (n.getType() == 0)
			{	
				node = n.getNode();
				if (node.getID() > max) max = node.getID();
				nodeValues.put(node.getID(), node);
				initialPR.put(node.getID(), node.getPageRank());
			}
			
			// if n is BCo
			else if (n.getType() == 1) 
			{
				//BCO.add(n.getBCo());
				if(BCoNodes.containsKey(n.getBCo().getDestination())){
					ArrayList<BoundaryCondition> temp = BCoNodes.get(n.getBCo().getDestination());
					temp.add(n.getBCo());
					BCoNodes.put(n.getBCo().getDestination(), temp);
				}
				else{
					ArrayList<BoundaryCondition> temp = new ArrayList<BoundaryCondition>();
					temp.add(n.getBCo());
					BCoNodes.put(n.getBCo().getDestination(), temp);
				}
				
			}
			
			// if n is BE
			else if (n.getType() == 2){	
				
				if(BENodes.containsKey(n.getBE().getDestination())){
					ArrayList<Integer> temp = BENodes.get(n.getBE().getDestination());
					temp.add(n.getBE().getSource());
					BENodes.put(n.getBE().getDestination(), temp);
				}
				else{
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(n.getBE().getSource());
				
					BENodes.put(n.getBE().getDestination(), temp);
				}
			}
		}
		//add nodes without edges
		int blockNodes = PageRank.returnBlockLimit(id.get()) - PageRank.returnBlockLimit(id.get()-1);
	
		if(blockNodes > nodeValues.size()){
			for(int j = PageRank.returnBlockLimit(id.get()-1)+1;j<PageRank.returnBlockLimit(id.get())+1;j++){
				if(!nodeValues.containsKey(j)){
					Node newNode = new Node(j,1.0/PageRank.NUM_NODES,id.get());
					nodeValues.put(j, newNode);
					initialPR.put(j, newNode.getPageRank());
					if(j > max)max = j;
					}
			}
		}
		
		ArrayList<Node> nodes = new ArrayList<Node>(nodeValues.values());
		
		double residualError = IterateBlockOnce(nodes,context);
		int i = 1;
		while(residualError > PageRank.THRESHOLD && i < max_iteration){
			i++;
			residualError = IterateBlockOnce(nodes,context);
		}
		residualError = 0.0;
		
		for(Node n : nodes){
			residualError += Math.abs(n.getPageRank() - initialPR.get(n.getID())) / n.getPageRank();
			context.write(new IntWritable(n.getID()), n);
		}
		residualError = residualError/nodes.size();
		
		long residualAsLong = (long) Math.floor(residualError * PageRank.LARGE_NUM);
		
		System.out.println("Block " + id +" max node is " + max + " and pagerank is " + 
				nodeValues.get(max).getPageRank());
		context.getCounter(PageRankCounter.TOTAL_VARIANCE).increment(residualAsLong);
		context.getCounter(PageRankCounter.ITERATION_COUNTER).increment(i);
		cleanup(context);
	}

	double IterateBlockOnce( ArrayList<Node> nodeList,Context context ) 
	{

		double d = 0.85;
				
		for (Node n:nodeList)
		{	
			n.setNewPageRank(0.0);

			if(BENodes.containsKey(n.getID())){
				for( int e: BENodes.get(n.getID()))
				{	
					Node source = nodeValues.get(e);	
					n.setNewPageRank((n.getPageRank() + 
							source.getOldPageRank()*1.0/source.getOrder()));
				
				}
			}
			if(BCoNodes.containsKey(n.getID())){
				for ( BoundaryCondition bco: BCoNodes.get(n.getID()))
				{
					n.setNewPageRank(n.getPageRank() + bco.getPageRank());
				}
			}
			n.setNewPageRank(d*n.getPageRank() + ((1.0-d)/PageRank.NUM_NODES));
		}
		

		double variance = 0.0;
		for (Node n:nodeList)
		{	
			variance = variance + (Math.abs(n.getOldPageRank() - n.getPageRank())/n.getPageRank());
			n.setOldPageRank(n.getPageRank());
		}
		return variance/nodeList.size();

	}
}


