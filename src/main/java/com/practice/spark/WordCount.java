package com.practice.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	 public static void main(String[] args) throws Exception {
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
	    	JavaSparkContext sc = new JavaSparkContext(conf);
	    	JavaRDD<String> lines = sc.textFile("/Users/abhinav/Desktop/DataSet/catalina.out");
	    	JavaPairRDD<String, Integer> wordCount = wordCountSortedbyFrequency(lines);
	    	wordCount.saveAsTextFile("/Users/abhinav/Desktop/DataSet/0001-count");
	    	sc.close();
	 }

	private static JavaPairRDD<String, Integer> wordCountSortedbyFrequency(JavaRDD<String> lines) {
		return lines.flatMap(s -> Arrays.asList(s.split(" ")))
		.map(w -> w.trim())
		.mapToPair(w -> new Tuple2<String , Integer>(w, 1))
		.reduceByKey((x,y) -> x+y) 
		.mapToPair(t -> new Tuple2<Integer , String>(t._2, t._1))
		.sortByKey(false)
		.mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
	}
}
