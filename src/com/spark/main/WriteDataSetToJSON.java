package com.spark.main;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WriteDataSetToJSON {

	public static class Employee implements Serializable{
		public String name;
		public int salary;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession session = SparkSession.builder().appName("Writing to JSON").getOrCreate();
		Encoder<Employee> encoder = Encoders.bean(Employee.class);
		String jsonPath ="../../../eclipse-workspace/SparkSample/resources/employees1.json";
		Dataset<Employee> ds = session.read().json(jsonPath).as(encoder);
		jsonPath ="../../../eclipse-workspace/SparkSample/resources/employees1_output.json";
		ds.write().json(jsonPath);
	}

}
