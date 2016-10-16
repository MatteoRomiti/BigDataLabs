package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.lang.reflect.Array;
import java.util.*;

public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab6");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Select the columns in the input file
        JavaPairRDD<String,Iterable<String>> transactions = logRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] features = s.split(",");

                return new Tuple2<>(features[2], features[1]);
            }
        }).groupByKey();
        

        JavaPairRDD<String,Integer> resultRDD = transactions.values().flatMapToPair(new PairFlatMapFunction<Iterable<String>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<String> products) {
                List<Tuple2<String, Integer>> results = new ArrayList<>();

                for(String p1 : products){
                    for (String p2 : products){
                        if(p1.compareTo(p2) > 0)
                            results.add(new Tuple2<>(p1+" "+p2, 1));
                    }
                }

                return results;
            }
        }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );
		

        //List<Tuple2<String, Integer>> topList = resultRDD.top(200);

//                , new Comparator<Tuple2<String, Integer>>() {
//            @Override
//            public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
//                return t2._2 - t1._2;
//            }
//        });

        //System.out.println(topList);

        // Store the result in the output folder
        resultRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> t) throws Exception {
                return t._2>1;
            }
        }).map(new Function<Tuple2<String,Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> t) throws Exception {
                return t._1+","+t._2;
            }
        }).saveAsTextFile(outputPath);

        // Close the Spark context
		sc.close();
	}
}
