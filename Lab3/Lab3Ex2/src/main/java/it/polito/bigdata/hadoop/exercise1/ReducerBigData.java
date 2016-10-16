package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String products="";

        // Iterate over the set of values and concatenate
        for (Text value : values) {
            products = products +","+ value;
        }
        context.write(key, new Text(products));
    }
}
