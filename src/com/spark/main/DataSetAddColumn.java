package com.spark.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.spark.main.JSONToDataSet.Employee;

public class DataSetAddColumn {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession session = SparkSession.builder().appName("Add a new column to DataSet").master("local[3]").getOrCreate();
		String jsonPath = "../../../eclipse-workspace/SparkSample/resources/employees1.json";
		Dataset<Row> ds = session.read().json(jsonPath);
		System.out.println("Present DataFrame");
		ds.show();
		Dataset<Row> newDs = ds.withColumn("new_col", functions.lit(1));
		System.out.println("New DataFrame");
		newDs.show();
	}

}
