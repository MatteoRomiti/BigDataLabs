package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    LongWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use a comma as delimiter.
    		// The split method returns an array of strings
            String[] words = value.toString().split(",");
            
            // Emit all the pairs of different products.
            for (int i = 1; i< words.length; i++){
            	for (int j = 1; j< words.length; j++){
            		if (words[i].compareTo(words[j]) > 0){ //count each pair of different products only once
                        context.write(new Text(words[i]+","+words[j]), new LongWritable(1));
            		}
            	}
            }
            
    }
}
