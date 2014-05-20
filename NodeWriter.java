import java.io.IOException;
import java.io.DataOutputStream;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class NodeWriter extends RecordWriter<IntWritable, Node> {

    private DataOutputStream out; 

    public NodeWriter(DataOutputStream out) throws IOException{
	this.out = out;
    }

    public synchronized void write(IntWritable key, Node value) throws IOException {
	    boolean keyNull   = key == null;
	    boolean valueNull = value == null;
	    
	    if(valueNull) { //Can't write a Null value
		return;
	    }
	    if(keyNull) {
		write(new IntWritable(value.getID()), value); //If we have a null key, then just use the Node's nodeid
	    }

	    String nodeRep = value.toString(); 
	    
	    out.writeBytes(nodeRep.trim() + "\n");
    }

    public synchronized void close(TaskAttemptContext ctxt) throws IOException{
        out.close();
    }
}