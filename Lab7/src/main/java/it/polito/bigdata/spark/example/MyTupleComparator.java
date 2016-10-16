package it.polito.bigdata.spark.example;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

class MyTupleComparator implements
        Comparator<Tuple2<String, Integer>>, Serializable {
    final static MyTupleComparator INSTANCE = new MyTupleComparator();
    // note that the comparison is performed on the key's frequency
    // assuming that the second field of Tuple2 is a count or frequency
    public int compare(Tuple2<String, Integer> t1,
                       Tuple2<String, Integer> t2) {
        //return -t1._2.compareTo(t2._2);    // sort descending
         return t1._2.compareTo(t2._2);  // sort ascending
    }
}
