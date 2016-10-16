package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    LongWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Identity mapper    		
            context.write(key, new LongWritable(Long.parseLong(value.toString())));            
    }
}
