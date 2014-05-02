import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class BlockMapper extends Mapper<IntWritable,Node,IntWritable, NodeOrBCo> {
	
	public void map (IntWritable id , Node n, Context context) throws IOException, InterruptedException
	{
		IntWritable blockID =  n.getBlockID();
		float R = n.getPageRank()/n.getOrder();
		for (Node neighbour:n.getBoundaryNeighbors())
		{	
			BoundaryCondition bc = new BoundaryCondition(n,neighbour,R);
			context.write(blockID,new NodeOrBCo(bc));
		}

		context.write(blockID,new NodeOrBCo(n));

	}
	
}