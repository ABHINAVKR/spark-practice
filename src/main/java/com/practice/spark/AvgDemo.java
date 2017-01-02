package com.practice.spark;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AvgDemo {
    public static class AvgCount implements Serializable {

        
		private static final long serialVersionUID = 1L;

		public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public int total;
        public int num;

        public float avg() {
            return total / (float) num;
        }
    }
    
    public static void main(String[] args) throws Exception {

    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Avg");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4),10);
        AvgCount initial = new AvgCount(0,0);
        AvgCount result = rdd.aggregate(initial, 
        		(AvgCount a, Integer x) ->{
        			a.total +=x;
        			a.num++;
        			return a;
        		}, (AvgCount a , AvgCount b ) -> {
        			a.total +=b.total;
        			a.num += b.num;
        			return a;
        		});
        System.out.println(result.avg());
        sc.close();
    }

}
