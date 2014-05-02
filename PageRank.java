import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

 

public class PageRank {

	BufferedReader br;
	ArrayList<Node> nodeList;
	static final double FROM_NET_ID = 0.734;
	static final double REJECT_MIN = 0.99*FROM_NET_ID;
	static final double REJECT_LIMIT = REJECT_MIN + 0.01;
	
	
	public PageRank(){
		
		try{
			
			String line;

			br = new BufferedReader(new FileReader("/home/leonardo/workspace/UDPWeb/src/nodes.txt"));
			nodeList = new ArrayList<Node>();
			while((line = br.readLine()) != null){
				String [] nodeArr = line.trim().split(" +");
				nodeList.add(new Node(Integer.parseInt(
						nodeArr[0]), Integer.parseInt(nodeArr[1])));
			}
			
			Node currentNode = null;
			br = new BufferedReader(new FileReader("/home/leonardo/workspace/UDPWeb/src/edges.txt"));
			PrintWriter f0 = new PrintWriter(new FileWriter("lr437edges.txt"));
			
			while((line = br.readLine()) != null){
				String [] arr = line.trim().split(" +");
				if (currentNode == null || Integer.parseInt(arr[0]) != currentNode.getID() ){
					currentNode = nodeList.get(Integer.parseInt(arr[0]));
				}
				if(selectInputLine(Double.parseDouble(arr[2]))){
					if(nodeList.get(Integer.parseInt(arr[1])).block == currentNode.block){
						currentNode.addBlockNeighbor(nodeList.get(Integer.parseInt(arr[1])));
					}
					else{
						currentNode.addBoundaryNeighbor(nodeList.get(Integer.parseInt(arr[1])));
					}
				}
			}
			
		} catch (IOException e){
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
	
	
	}
	public static void main(String[] args) throws IOException {
		int numRepititions = 5; //The number of PageRank passes to run

		for(int i = 0; i < numRepititions; i++) { //We need to run 2 iterations to make 1 pass
		    
		    //Run the right job for the current pass
		    JobConf conf = new JobConf(PageRank.class);
		    conf.setJobName("pagerank");
		    conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Float.class);
			conf.setMapperClass((Class<? extends Mapper>) BlockMapper.class);
			conf.setCombinerClass((Class<? extends Reducer>) BlockReducer.class);
			conf.setReducerClass((Class<? extends Reducer>) BlockReducer.class);
		    String inputPath = i == 0 ? "input" : "stage" + (i-1);
		    String outputPath = "stage" + i;
		    conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, new Path(inputPath));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath));


		    JobClient.runJob(conf);
		  
		}
}
	boolean selectInputLine(double x){
		return ( ((x >= REJECT_MIN) && (x < REJECT_LIMIT)) ? false : true );
	}
}


