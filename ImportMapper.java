import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ImportMapper extends Mapper<IntWritable,IntWritable,IntWritable, IntWritable> {
	/**
	 * Identity Function to import and format data
	 */
	public void map (IntWritable id , IntWritable n, Context context) throws IOException, InterruptedException
	{		
			if(n.get() != -1){
				context.write(id,n);
			}
	}
	
}