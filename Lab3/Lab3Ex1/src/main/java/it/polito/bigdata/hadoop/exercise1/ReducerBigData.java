package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                LongWritable,    // Input value type
                Text,           // Output key type
                LongWritable> {  // Output value type
	
	TreeSet<PairOfProducts> topKTree;
    
	protected void setup(Context context){
		topKTree = new TreeSet<PairOfProducts>();
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException
	{
		// emit the local top K list
		for (PairOfProducts e : topKTree){
			context.write(new Text(e.getPair()), new LongWritable(e.getCount()));
		}
	}
	
	@Override
    protected void reduce(
        Text key, // Input key type
        Iterable<LongWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer k = 100; // the k for the top k list
		Long totcount = 0L;

        // Iterate over the set of values 
        for (LongWritable value : values) {
            totcount+=value.get();
        }
        
        if(topKTree.size() < k ){
        	topKTree.add(new PairOfProducts(new Text(key), new Long(totcount)));
        } else if(topKTree.first().getCount() < totcount) {
        	topKTree.remove(topKTree.first());
        	topKTree.add(new PairOfProducts(new Text(key), new Long(totcount)));
        } // else the element is not in the top K: do nothing
        
        
    }
}
