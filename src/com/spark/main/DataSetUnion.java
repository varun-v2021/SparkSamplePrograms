package com.spark.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetUnion {

	/*Spark provides union() method in Dataset class to concatenate or append a Dataset to another. Dataset Union can only be performed on Datasets with the same number of columns.
	 * 
	 * */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession session = SparkSession.builder().appName("DataSet Union").master("local[3]").getOrCreate();
		Dataset<Row> ds1 = session.read().json("../../../eclipse-workspace/SparkSample/resources/employees1.json");
		Dataset<Row> ds2 = session.read().json("../../../eclipse-workspace/SparkSample/resources/employees2.json");
		// print dataset
		System.out.println("Dataset 1\n==============");
		ds1.show();
		System.out.println("Dataset 2\n==============");
		ds1.show();

		// concatenate datasets
		Dataset<Row> ds3 = ds1.union(ds2);

		System.out.println("Dataset 3 = Dataset 1 + Dataset 2\n==============================");
		ds3.show();

		session.stop();
	}

}
