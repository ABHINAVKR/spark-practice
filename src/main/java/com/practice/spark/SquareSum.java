package com.practice.spark;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SquareSum {

	public static void main(String[] args) {
		List<Integer> list = new LinkedList<Integer>();
		for(int i =0; i< Math.pow(10, 1);i++){
			list.add(i);
		}
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Hello World");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	calcSquareSum(list, sc);
    	//exampleFlatMap(sc);
	}
	
	// Example of map
	public static void calcSquareSum(List<Integer> list,JavaSparkContext sc ){
		JavaRDD<Integer> rdd = sc.parallelize(list);
		JavaRDD<Integer> mappedRDD = rdd.map(x -> x*x);
			Integer squareSum = mappedRDD.reduce((x,y) -> x+y);
			System.out.println(StringUtils.join(mappedRDD.collect(), ","));
			System.out.println(squareSum);
	}

	public static void exampleFlatMap(JavaSparkContext sc){
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("coffee panda","happy panda","happiest panda party"));
			JavaRDD<String> flatMap = lines.flatMap(x -> Arrays.asList(x.split(" ")));
			JavaRDD<String[]> map = lines.map(x -> x.split(" "));
			System.out.println(flatMap.collect());
			map.collect().forEach(x -> System.out.println(Arrays.toString(x)));
	}
	
}
