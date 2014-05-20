import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;


public class NodeImport extends RecordReader<IntWritable, IntWritable> {
    private LineRecordReader lineReader; //Does the actual reading for us
    private LongWritable lineKey; // The key from lineReader
                                  // Contains the line number we're reading from
    private Text lineValue; // The value from lineReader
    
    private IntWritable curKey; // The current key of our NodeRecordReader, the current Node's nodeid
    private IntWritable curVal; // The current value of our NodeRecordReader, the current Node
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

	
	String[] pieces = lineValue.toString().trim().split(" +");

    curKey = new IntWritable(Integer.parseInt(pieces[0]));
   
    
    
    if(PageRank.selectInputLine(Double.parseDouble(pieces[2]))){
    	curVal = new IntWritable(Integer.parseInt(pieces[1]));
    }
    else{
    	curVal = new IntWritable(-1);
    }
    
	return true;
    }
		

    public IntWritable getCurrentKey() {
	return curKey;
    } 

    public IntWritable getCurrentValue() {
	return curVal;
    }
    
    public void close() throws IOException {
	lineReader.close();
    }

    public float getProgress() throws IOException {
	return lineReader.getProgress();
    }
}