import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;


public class NodeReader extends RecordReader<IntWritable, Node> {
    private LineRecordReader lineReader; //Does the actual reading for us
    private LongWritable lineKey; // The key from lineReader
                                  // Contains the line number we're reading from
    private Text lineValue; // The value from lineReader
    
    private IntWritable curKey; // The current key of our NodeRecordReader, the current Node's nodeid
    private Node curVal; // The current value of our NodeRecordReader, the current Node
    private long end = 0, start = 0, pos = 0, maxLineLength; //Line tracking variables

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
	//To initialize, create and initialize our lineReader
	lineReader = new LineRecordReader();
	lineReader.initialize(genericSplit, context);
    }

    public boolean nextKeyValue() throws IOException {
	if(!lineReader.nextKeyValue()) {//If we're out of lines, we're done
	    return false;
	}
	
	//get the correct current key and value
	lineKey = lineReader.getCurrentKey();
	lineValue = lineReader.getCurrentValue();

	
	String[] pieces = lineValue.toString().trim().split("\\s+");
	if(pieces.length !=  5) {
	    throw new IOException("Given poorly-formatted record: " + lineValue.toString()); //If this isn't what's going on, then we need to report an error.
	}
	
	int nodeid = Integer.parseInt(pieces[0]);
	curKey = new IntWritable(nodeid);
	
	Node n = new Node(nodeid,Double.parseDouble(pieces[2]),
			Integer.parseInt(pieces[1]));
	
	
	n.setBlockNeighbors(pieces[3]);
	n.setBoundaryNeighbors(pieces[4]);
	curVal = n;

	return true;
    }
		

    public IntWritable getCurrentKey() {
	return curKey;
    }

    public Node getCurrentValue() {
	return curVal;
    }
    
    public void close() throws IOException {
	lineReader.close();
    }

    public float getProgress() throws IOException {
	return lineReader.getProgress();
    }
}