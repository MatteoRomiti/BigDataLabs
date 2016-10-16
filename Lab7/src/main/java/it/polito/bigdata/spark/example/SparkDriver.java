package it.polito.bigdata.spark.example;

import com.google.common.base.Optional;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple4;


import java.lang.reflect.Array;
import java.util.*;

public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
        String inputPathHTTP;

		inputPath=args[0];
		inputPathHTTP=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Filter the header
        JavaRDD<String[]> records = logRDD.map(new Function<String, String[]>() {
            public String[] call(String s) {
                String[] features = s.split("\\s+");


                return features;
            }
        }).filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                return strings.length==32 && !strings[0].startsWith("#");
            }
        });

        JavaRDD<String> logHttpRDD = sc.textFile(inputPathHTTP);

		// Filter the header
        JavaRDD<String[]> recordsHttp = logHttpRDD.map(new Function<String, String[]>() {
            public String[] call(String s) {
                String[] features = s.split("\\s+");


                return features;
            }
        }).filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                return !strings[0].contains("#");
            }
        });



        //EXERCISE 1.

        // Filter HTTP and HTTPs connection
        JavaRDD<String[]> httpRecords = records.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                Integer conntype = Integer.parseInt(strings[30]);
                Integer port = Integer.parseInt(strings[15]);
                return (conntype == 1 || (conntype == 8192 && port == 443));
            }
        }).cache();

        // Count the number of lines per FQDN
        JavaPairRDD<String, Integer> pairs = httpRecords.mapToPair(new PairFunction<String[], String, Integer>() {
            public Tuple2<String, Integer> call(String[] s) { return new Tuple2<String, Integer>(s[31], 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

        // Take only the top 10
        final List<Tuple2<String, Integer>> top10 = counts.top(10, new MyTupleComparator());

        final HashSet<String> top10sites = new HashSet<>();
        for (Tuple2<String, Integer> i : top10){
            top10sites.add(i._1);
        }

        // Broadcast the top 10 list to the machines (save bandwidth)
        final Broadcast<HashSet<String>> top10bc = sc.broadcast(top10sites);

        // Filter the records belonging to the top 10
        JavaRDD<String[]> top10HttpRecords = httpRecords.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                return top10bc.value().contains(strings[31]);
            }
        });

        // Count the bytes in upload
        JavaPairRDD<String, Long> top10bytesUp = top10HttpRecords.mapToPair(new PairFunction<String[], String, Long>() {

            @Override
            public Tuple2<String, Long> call(String[] strings) throws Exception {
                return new Tuple2<String, Long>(strings[31], Long.parseLong(strings[6]));
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a+b;
            }
        });

        // Count the bytes in download
        JavaPairRDD<String, Long> top10bytesDown = top10HttpRecords.mapToPair(new PairFunction<String[], String, Long>() {

            @Override
            public Tuple2<String, Long> call(String[] strings) throws Exception {
                return new Tuple2<String, Long>(strings[31], Long.parseLong(strings[20]));
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a+b;
            }
        });

        System.out.println("Top 10 sites - upload: \n"+top10bytesUp.collectAsMap());
        System.out.println("Top 10 sites - download: \n"+top10bytesDown.collectAsMap());

        // EXERCISE 2.

        // Filter the records of the application we are interested in
        JavaRDD<String[]> usageRecords = records.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                Integer conntype = Integer.parseInt(strings[30]);
                Integer port = Integer.parseInt(strings[15]);
                if (conntype == 1) return true;
                if (conntype == 1024) return true;
                if (conntype == 4096) return true;
                if (conntype == 8192) if ((port == 443) || (port == 993) || (port == 995) || (port == 465)) return true;
                return false;
            }
        }).cache();

        // Count the usage per hour
        JavaPairRDD<Integer, Long> usagePerHour = usageRecords.mapToPair(new PairFunction<String[], Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(String[] strings) throws Exception {
                Calendar cal = Calendar.getInstance();
                String msEpoch = strings[28];
                cal.setTimeInMillis((long) Double.parseDouble(msEpoch));
                int hour =  cal.get(Calendar.HOUR_OF_DAY);
                long totBytes = Long.parseLong(strings[6]) + Long.parseLong(strings[20]);
                return new Tuple2<Integer, Long>(hour, totBytes);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a+b;
            }
        });

        // Count the usage per day
        JavaPairRDD<Integer, Long> usagePerDay = usageRecords.mapToPair(new PairFunction<String[], Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(String[] strings) throws Exception {
                Calendar cal = Calendar.getInstance();
                String msEpoch = strings[28];
                cal.setTimeInMillis((long) Double.parseDouble(msEpoch));
                int hour =  cal.get(Calendar.DAY_OF_WEEK);
                long totBytes = Long.parseLong(strings[6]) + Long.parseLong(strings[20]);
                return new Tuple2<Integer, Long>(hour, totBytes);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a+b;
            }
        });

        System.out.println(usagePerHour.collectAsMap());
        System.out.println(usagePerDay.collectAsMap());

        // EXERCISE 3.

        // Key by <client IP, client port, server IP, server port>:
        JavaPairRDD<Tuple4, String[]> recordsHttpPairRDD = recordsHttp.mapToPair(new PairFunction<String[], Tuple4, String[]>() {

            @Override
            public Tuple2<Tuple4, String[]> call(String[] strings) throws Exception {
                Tuple4<String, String, String, String> key = new Tuple4<>(strings[0], strings[1], strings[2], strings[3]);
                return new Tuple2<Tuple4, String[]>(key, strings);
            }
        });

        recordsHttpPairRDD.cache(); // we will use it in ex.4 as well

        // For each key, count the requests and the responses
        // Keep the count in a tuple: (#requests, #responses)
        JavaPairRDD<Tuple4, Tuple2<Integer, Integer>> numReqsAndRespByKey = recordsHttpPairRDD.aggregateByKey(new Tuple2<>(0, 0), new Function2<Tuple2<Integer, Integer>, String[], Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> agg, String[] r) throws Exception {
                if (r[5].equals("HTTP")) {
                    return new Tuple2<Integer, Integer>(1, 0);
                }
                if (r[5].equals("GET") || r[5].equals("POST") || r[5].equals("HEAD")) {
                    return new Tuple2<Integer, Integer>(0, 1);
                }
                return new Tuple2<Integer, Integer>(0, 0);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) throws Exception {
                return new Tuple2<Integer, Integer>(a._1 + b._1, a._2 + b._2);
            }
        });

        // Filter the rows that do not have a matching number of requests/responses:
        long countUnmatch = numReqsAndRespByKey.filter(new Function<Tuple2<Tuple4, Tuple2<Integer, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple4, Tuple2<Integer, Integer>> t) throws Exception {
                return t._2._1.equals(t._2._2);
            }
        }).count();


        // Count the number of requests
        long numRequests = numReqsAndRespByKey.map(new Function<Tuple2<Tuple4,Tuple2<Integer,Integer>>, Long>() {
            @Override
            public Long call(Tuple2<Tuple4, Tuple2<Integer, Integer>> t) throws Exception {
                return (long) t._2._1;
            }
        }).reduce(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a+b;
            }
        });

        System.out.println("# of HTTP requests: "+ numRequests);
        System.out.println("# of unmatched http requests/responses (total): "+countUnmatch);

        // EXERCISE 4.

        // Filter requests and map key to the FQDN
        JavaPairRDD<Tuple4, String> httpRequests = recordsHttpPairRDD.filter(new Function<Tuple2<Tuple4, String[]>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple4, String[]> r) throws Exception {
                return (r._2[5].equals("GET") || r._2[5].equals("POST") || r._2[5].equals("HEAD"));
            }
        }).mapValues(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[6];
            }
        });

        // Filter responses and map key to the # of errors
        // N.B.: we compute the total # of errors per key
        // and filter out the rows without errors to
        // reduce the load of the join()
        JavaPairRDD<Tuple4, Integer> httpResponses = recordsHttpPairRDD.filter(new Function<Tuple2<Tuple4, String[]>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple4, String[]> r) throws Exception {
                return r._2[5].equals("HTTP");
            }
        }).mapValues(new Function<String[], Integer>() {
            @Override
            public Integer call(String[] strings) throws Exception {
                int errors = 0;
                if (strings[6].matches("[45]..")){
                    errors=1;
                }
                return errors;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a+b;
            }
        }).filter(new Function<Tuple2<Tuple4, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple4, Integer> t) throws Exception {
                return t._2 > 0;
            }
        });





        JavaPairRDD<String, Integer> numErrorsByFQDN;
        numErrorsByFQDN = httpRequests.join(httpResponses).mapToPair(
                new PairFunction<Tuple2<Tuple4, Tuple2<String, Integer>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Tuple4, Tuple2<String, Integer>> t) throws Exception {
                        return t._2;
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) throws Exception {
                        return a + b;
                    }
                }
        );

        numErrorsByFQDN.cache(); //we use it twice

        // Count total number of errors
        Integer totErrors = numErrorsByFQDN.aggregate(0, new Function2<Integer, Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer call(Integer count, Tuple2<String, Integer> t) throws Exception {
                return count + t._2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer count, Integer count2) throws Exception {
                return count + count2;
            }
        });


        //Select the top 10 FQDNs by number of errors
        List<Tuple2<String, Integer>> top10err = numErrorsByFQDN.top(10, new MyTupleComparator());

        System.out.println("Percentage of errors: "+100.0*totErrors/numRequests);
        System.out.println("Top 10 FQDN by # of errors: "+top10err);

        // Close the Spark context
		sc.close();
	}
}
