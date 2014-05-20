
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;


public class NodeImportFormat extends FileInputFormat<IntWritable, IntWritable> {
    public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit input, TaskAttemptContext ctx) throws IOException {
	return new NodeImport();//Simply construct and return a NodeRecordReader
    }
}