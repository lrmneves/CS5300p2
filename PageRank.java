import java.util.ArrayList;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
 

public class PageRank {
	
	static final double FROM_NET_ID = 0.734;
	static final double REJECT_MIN = 0.99*FROM_NET_ID;
	static final double REJECT_LIMIT = REJECT_MIN + 0.01;
	static final int NUM_NODES = 685230;
	static final int NUM_BLOCKS = 68;
	static final double THRESHOLD = 0.001; 
	
	public static final double LARGE_NUM = Math.pow(10, 5);
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int numRepetitions = 10; //The number of PageRank passes to run -1 since first is import.  Converges in 6 iterations
		
		for(int i = 0; i < numRepetitions; i++) {
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "pagerank" + i);
			
			if (i != 0){      
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Node.class);
			
			job.setJarByClass(PageRank.class);

			job.setMapOutputKeyClass(IntWritable.class); 
			job.setMapOutputValueClass(NodeOrBCoOrBE.class);
			
			job.setMapperClass(BlockMapper.class);
			job.setReducerClass(BlockReducer.class);

			job.setInputFormatClass(NodeInputFormat.class);
			job.setOutputFormatClass(NodeOutputFormat.class);
			}
			else{
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				
				job.setJarByClass(PageRank.class);

				job.setMapOutputKeyClass(IntWritable.class); 
				job.setMapOutputValueClass(IntWritable.class);
				
				job.setMapperClass(ImportMapper.class);
				job.setReducerClass(ImportReducer.class);

				job.setInputFormatClass(NodeImportFormat.class);
				job.setOutputFormatClass(NodeExportFormat.class);
			}
			String inputPath = i == 0 ? "input" : "stage" + (i-1);
			String outputPath = "stage" + i;
			
			if (args.length > 0){
				inputPath = args[0] + inputPath;
				outputPath = args[0] + outputPath;
			}

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			job.waitForCompletion(true);
		
			
			if(i != 0){
			double totvariance = job.getCounters().findCounter(PageRankCounter.TOTAL_VARIANCE).getValue();
		    double average_variance  = (totvariance/(LARGE_NUM))/NUM_BLOCKS;
		    int average_iteration = (int) job.getCounters().findCounter(PageRankCounter.ITERATION_COUNTER)
		    		.getValue();

		    System.out.println("Residual error: " + average_variance);
		    System.out.println("Average Block Iteration: " + average_iteration/NUM_BLOCKS);
		    if(PageRank.THRESHOLD >= average_variance){
		    	break;
		    }
			}
			
		}
}
	static boolean selectInputLine(double x){
		return ( ((x >= REJECT_MIN) && (x < REJECT_LIMIT)) ? false : true );
	}
	
	public static IntWritable returnBlock(int nodeid){
				
			int partitionSize = 10000;
			int[] blocks = { 0, 10328, 20373, 30629, 40645,
					50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
					130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
					212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
					293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
					374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
					454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
					534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
					616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };
			//calculate a random blockID
			//int blockID = Integer.toString(nodeid).hashCode() % blocks.length;
			int blockID = (int) Math.floor(nodeid / partitionSize);
			//make sure the id is correct
			int correctBlock = blocks[blockID];
			if (nodeid < correctBlock) {
				blockID--;
						
			}
			return new IntWritable(blockID);

	}
	public static int returnBlockLimit(int blockid){
		
		int[] blocks = { 0, 10328, 20373, 30629, 40645,
				50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
				130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
				212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
				293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
				374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
				454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
				534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
				616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };
		
		
		return blocks[blockid+1]-1;
		
				
		
	}
	
	
}


