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


public class ImportReducer extends Reducer<IntWritable,IntWritable, IntWritable, Text>{
	
	/**
	 * Initialize file to be [id,block,pagerank,blockneighbors(CSV),boundaryneighbors(CSV)]
	 */
	public void reduce(IntWritable id, Iterable<IntWritable> edges,Context context) throws IOException, InterruptedException
	{

		StringBuilder blockNeighbors = new StringBuilder();
		StringBuilder boundaryNeighbors = new StringBuilder(); 
	
		for (Iterator<IntWritable> i = edges.iterator();i.hasNext();){
			
			 int neigh = i.next().get();
			
			 
			 if (PageRank.returnBlock(neigh).get() == PageRank.returnBlock(id.get()).get()){
				 blockNeighbors.append(neigh + ",");
			 }
			 else{
				 boundaryNeighbors.append( neigh + ",");
			 }
    	}
		double initialPR = (double) (1.0/PageRank.NUM_NODES) ;
		//export node as id block pagerank blockneigh boundaryneigh
	
		
		Text nodeinfo = new Text(id + " " + PageRank.returnBlock(id.get()) + " " + String.format("%.15f", initialPR) + " " +
				(blockNeighbors.length() != 0 ? blockNeighbors.toString().substring(
						0, blockNeighbors.length()-1) : "-1" )+ " " +
				(boundaryNeighbors.length() != 0 ?boundaryNeighbors.toString().substring(
						0, boundaryNeighbors.length()-1):"-1")+"\n");
		
		
		context.write(id, nodeinfo);
	}
}


