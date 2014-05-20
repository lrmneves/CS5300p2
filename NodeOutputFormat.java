import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class NodeOutputFormat extends FileOutputFormat<IntWritable, Node> {
    
    //Describes how to get a NodeRecordWriter object.
    //Note the use of the Factory pattern
    public RecordWriter<IntWritable, Node> getRecordWriter(TaskAttemptContext ctxt) throws IOException, InterruptedException {
	Path file = FileOutputFormat.getOutputPath(new JobContext(ctxt.getConfiguration(), ctxt.getJobID())); //Get the path of the directory we're supposed to be writing to.
	//file = new Path(file.toString() + "/output.txt"); //Find the Path of the file we're supposed to write to
	file = new Path(file.toString() + FileOutputFormat.getUniqueFile(ctxt, "/output", ".txt"));
	
	String uriStr =  "s3n://cs5300lr437mapreduce/";
	URI uri = URI.create(uriStr);
	FileSystem fs = FileSystem.get(uri, ctxt.getConfiguration());	
	//FileSystem fs = FileSystem.get(ctxt.getConfiguration()); //Create a filesystem object for HDFS
	
	
	FSDataOutputStream fileOut = fs.create(file); //And get a output stream for our file!
	
	if (fs.exists(file))
	{
		fileOut = fs.append(file); 
	}
	else
	{
		fileOut = fs.create(file); //And get a output stream for our file!
	}
	
	return new NodeWriter(fileOut); // Now we can use that stream for our writer
    }

}