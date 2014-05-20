import java.io.IOException;
import java.io.DataOutputStream;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class NodeExport extends RecordWriter<IntWritable, Text> {

    private DataOutputStream out; 

    public NodeExport(DataOutputStream out) throws IOException{
	this.out = out;
    }

    public synchronized void write(IntWritable key, Text value) throws IOException {
	    boolean keyNull   = key == null;
	    boolean valueNull = value == null;
	    
	    if(valueNull) { //Can't write a Null value
		return;
	    }
	    if(keyNull) {
	    	//If no key, write with the key in value
			write(new IntWritable(Integer.parseInt(
					String.valueOf(value.charAt(0)))), value); //If we have a null key, then just use the Node's nodeid
	    }
	    out.writeBytes(value.toString());//And write out the resulting record!
	  
    }

    public synchronized void close(TaskAttemptContext ctxt) throws IOException{
        out.close();
    }
}