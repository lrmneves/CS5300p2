import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.*;


public class BlockReducer extends Reducer<IntWritable,NodeOrBCo, IntWritable, Node>{
	
	
	public void reduce(IntWritable id, Iterable<NodeOrBCo> nodeList,Context context) throws IOException, InterruptedException
	{
		ArrayList<Edge> BE = new ArrayList<Edge>();
		ArrayList<BoundaryCondition> BCO = new ArrayList<BoundaryCondition>();
		HashSet<Node> memberNodes = new HashSet<Node>();
		Node node = null;
		for (Iterator<NodeOrBCo> i = nodeList.iterator();i.hasNext();)
		{	
			NodeOrBCo n = i.next();
			if (n.isNode())
			{	
				node = n.getNode();
				memberNodes.add(node);
				for (Node neighbour: node.getBlockNeighbors())
				{
					BE.add(new Edge(node,neighbour));
				}
				/* Why? Dont we have to handle only the block nodes here?
				for (Node neighbour:n.boundaryNeighbours())
				{
					BE.add(n,neighbour);
				}
				*/

			}
			else //n is a boundary condition
			{
				BCO.add(n.getBCo());
			}
		}
		IterateBlockOnce(id,BE,BCO,new ArrayList<Node>(memberNodes));
		context.write(id, node);

	}

	void IterateBlockOnce(IntWritable BlockID, ArrayList<Edge> BE, 
			ArrayList<BoundaryCondition> BCO, ArrayList<Node> nodeList ) 
	{

		float d = 0.85f;
		ArrayList<Integer> NPR = new ArrayList<Integer>();
		for (Node n:nodeList)
		{
			n.setNewPageRank(0);
		}

		for (Node n:nodeList)
		{
			for( Edge e:BE)
			{
				if ( e.getDestination().equals(n))
				{	
					Node source = e.getSource();
					n.setNewPageRank(n.getPageRank() + source.getOldPageRank()/source.getOrder());
				}
			}
			for ( BoundaryCondition bco: BCO)
			{
				if ( bco.getDestination().equals(n))
				{
					Node source = bco.getSource();
					float R = n.getPageRank()/n.getOrder();
					n.setNewPageRank(source.getPageRank() + R);
				}
			}

			n.setNewPageRank(d*n.getPageRank() + (1-d)/n.getOrder());
		}

		for (Node n:nodeList)
		{
			n.setNewPageRank(n.getOldPageRank());
		}

	}
}


