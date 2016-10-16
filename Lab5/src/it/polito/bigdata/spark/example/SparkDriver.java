package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		String startWord;
		
		inputPath=args[0];
		outputPath=args[1];
		startWord = args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exercise #30");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Solution based on an named class
		// An object of the StartFilter is used to filter the content of the RDD.
		// Only the elements of the RDD satisfying the filter imposed by means 
		// of the call method of the StartFilter class are included in the
		// resultRDD RDD
		JavaRDD<String> resultRDD = logRDD.filter(new StartFilter(startWord));
		
		// Store the result in the output folder
		resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
