import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// main mapper task 
public class BlockMapper extends Mapper<IntWritable,Node,IntWritable, NodeOrBCoOrBE> {
	
	public void map (IntWritable id , Node n, Context context) throws IOException, InterruptedException
	{
		int blockID =  n.getBlockID();
		
		ArrayList<Integer> blockNeighbors = n.getBlockNeighbors();
		ArrayList<Integer> boundaryNeighbors = n.getBoundaryNeighbors();
		
		// emit the neighbors that are in the same block as the current node
		for (Integer bl : blockNeighbors){
			if (bl != -1){
				Edge e = new Edge(id.get(),bl);
				context.write(new IntWritable(blockID), new NodeOrBCoOrBE(e));
			}
		}
		
		// emit the neighbors that are on the boundary of the block of the current node
		for (Integer bco : boundaryNeighbors){
			if (bco !=-1){
				BoundaryCondition b = new BoundaryCondition(id.get(),bco,(n.getPageRank()/n.getOrder()));
				context.write(PageRank.returnBlock(bco.intValue()), new NodeOrBCoOrBE(b));
			}
		}
		
		// emit the current node itself
		context.write(new IntWritable(blockID),new NodeOrBCoOrBE(n));

	}
	
}