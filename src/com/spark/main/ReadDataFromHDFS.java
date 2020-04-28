package com.spark.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ReadDataFromHDFS {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf  = new SparkConf().setAppName("HDFS File Read App");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> data = context.textFile("hdfs://localhost:9000/wordcount/input/sample.txt");
		for(String s : data.collect()) {
			System.out.println(s);
		}
		
	}

}
