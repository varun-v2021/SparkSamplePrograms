package com.spark.main;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.ImmutableList;

import scala.Tuple2;

public class SparkWordCount2 {

	/*
	 * Command to execute
	 * 
	 * cd ~/Downloads/spark-2.4.5-bin-hadoop2.7/bin
	 * ./spark-submit --class com.spark.main.SparkWordCount2 --master local /home/varun/eclipse-workspace/SparkSample/target/SparkSample-0.0.1-SNAPSHOT.jar
	 * 
	 * */
	public static void main2(String[] args) {

		//local[2] - here master can use 2 threads
		SparkConf conf = new SparkConf().setAppName("Spark Sample App").setMaster("local[2]").set("spark.executor.memory","2g");
		// TODO Auto-generated method stub
		//JavaSparkContext sc = new JavaSparkContext(); //this can also be used, but it is without custom conf
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parallelized with 2 partitions
		JavaRDD<String> rddX = sc.parallelize(
				Arrays.asList("spark rdd example", "sample example"),
				2);

		// collect RDD for printing
		for(String line:rddX.collect()){
			System.out.println("* "+line);
		}

		//OR

		// apply a function for each element of RDD
		rddX.foreach(item -> {
			System.out.println("*-- "+item); 
		});

		rddX.foreach(new VoidFunction<String>(){ 
			public void call(String line) {
				System.out.println("*-> "+line); 
			}});

		// map operation will return List of Array in following case
		JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
		List<String[]> listUsingMap = rddY.collect();

		// flatMap operation will return list of String in following case
		JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
		// collect RDD for printing
		for(String line:rddY2.collect()){
			System.out.println("# "+line);
		}

		List<String> listUsingFlatMap = rddY2.collect();

		//Reading from a Text file

		// provide path to input text file
		String path = "../../../eclipse-workspace/SparkSample/resources/file01.txt";

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile(path);

		// collect RDD for printing
		for(String line:lines.collect()){
			System.out.println(line);
		}


		//TO READ JSON DATA configure spark
		//Get DataFrameReader of the SparkSession.spark.read()
		//Use DataFrameReader.json(String jsonFilePath) to read the contents of JSON to Dataset<Row>.spark.read().json(jsonPath)
		//Use Dataset<Row>.toJavaRDD() to convert Dataset<Row> to JavaRDD<Row>.spark.read().json(jsonPath).toJavaRDD()
		// if multiline = true option is removed then null value will be appended to the data in output
		SparkSession spark = SparkSession
				.builder()
				.appName("Spark Example - Read JSON to RDD")
				.master("local[2]")
				.getOrCreate();

		// read list to RDD
		String jsonPath = "../../../eclipse-workspace/SparkSample/resources/employees.json";
		JavaRDD<Row> items = spark.read().option("multiline", "true").json(jsonPath).toJavaRDD();

		items.foreach(item -> {
			System.out.println(item); 
		});

		//
		//Setting 4 threads for master
		// IMPORTANT - THIS LINE DOESN'T THROW COMPILATION ERROR AS THERE IS ALREADY A SPARKCONF DEFINED AT THE START OF THE PROGRAM
		//BUT DURING RUNTIME, THERE WILL BE AN ERROR 'ONLY ONE SPARKCONTEXT MAY BE RUNNING IN THIS JVM'
		//TO CONTINUE PROGRAM, WE NEED TO set spark.driver.allowMultipleContexts = true.
		//BUT IT IS NOT ADVISABLE TO CREATE ANOTHER SPARKCONTEXT WHEN THERE ALREADY ONE ACTIVE
		//WE CAN CREATE CONTEXT FOR SQL, STREAMING, HIVE ETC
		//HENCE WE NEED TO STOP THE ACTIVE CONTEXT AND CREATE A NEW ONE WITH NEW CONFIGS.

		sc.stop(); // STOPPING THE PRESENT CONTEXT TO CREATE A NEW ONE
		//ON DOING THIS THERE WILL BE NO RUNTIME ERROR/EXCEPTION


		SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
				.setMaster("local[4]").set("spark.executor.memory","2g").set("spark.driver.allowMultipleContexts", "true");
		// start a spark context
		sc = new JavaSparkContext(sparkConf);
		// sample collection
		List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10); //partitioning value is optional

		// parallelize the collection to two partitions
		JavaRDD<Integer> rdd = sc.parallelize(collection);

		System.out.println("Number of partitions : "+rdd.getNumPartitions());

		rdd.foreach(new VoidFunction<Integer>(){ 
			public void call(Integer number) {
				System.out.println(number); 
			}});

		sc.stop();

		//local[2] - here master can use 2 threads
		conf = new SparkConf().setAppName("Spark Sample App").setMaster("local[2]").set("spark.executor.memory","2g");
		// TODO Auto-generated method stub
		//JavaSparkContext sc = new JavaSparkContext(); //this can also be used, but it is without custom conf
		sc = new JavaSparkContext(conf);

		//READING MULTIPLE FILES
		path = "../../../eclipse-workspace/SparkSample/resources/{file01.txt,file02.txt}";

		// read text file to RDD
		lines = sc.textFile(path);

		// collect RDD for printing
		for(String line:lines.collect()){
			System.out.println(line);
		}

		//READING MULTIPLE FILES
		path = "../../../eclipse-workspace/SparkSample/resources/*";

		// read text file to RDD
		lines = sc.textFile(path);

		// collect RDD for printing
		for(String line:lines.collect()){
			System.out.println(line);
		}

		//CUSTOM CLASS OBJECT
		// prepare list of objects
		List<Person> personList = ImmutableList.of(
				new Person("Arjun", 25),
				new Person("Akhil", 2));

		// parallelize the list using SparkContext
		JavaRDD<Person> perJavaRDD = sc.parallelize(personList);

		for(Person person : perJavaRDD.collect()){
			System.out.println(person.name);
		}

		//Using map

		JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(14,21,88,99,455));

		// map each line to number of words in the line
		JavaRDD<Double> log_values = numbers.map(x -> Math.log(x)); 

		// collect RDD for printing
		for(double value:log_values.collect()){
			System.out.println(value);
		}

		path = "../../../eclipse-workspace/SparkSample/resources/sample.txt";
		lines = sc.textFile(path);

		// map each line to number of words in the line
		JavaRDD<Integer> n_words = lines.map(x -> x.split(" ").length); 

		// collect RDD for printing
		for(int n:n_words.collect()){
			System.out.println(n);
		}

		//function that could return multiple elements to new RDD for each of the element of source RDD

		// flatMap each line to words in the line
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator()); 

		// collect RDD for printing
		for(String word:words.collect()){
			System.out.println(word);
		}

		//WORKING ON MAPTOPAIR API of SPARK
		// read list to RDD
		List<String> data = Arrays.asList("Learn", "Apache", "Spark", "with", "Tutorial Kart");
		words = sc.parallelize(data, 1);

		JavaRDD<String> words_new = words.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		for(String s : words_new.collect()) {
			System.out.println(s);
		}

		//Tuple2 is called a pair Tuple2 < T1 ,T2>
		// T1 value is represented by _1 i.e. first element in the tuple
		// T2 value is represented by _2 i.e. second element	in the tuple
		//this transformation produces PairRDD, that is, an RDD consisting of key and value pairs
		//with first element the word from lines and second element its (word) length
		JavaPairRDD<String, Integer> wordsPair = words_new.mapToPair(s -> new Tuple2<>(s,s.length())); 

		//Filtering to get data whose second element in the tuple is 5
		Function<Tuple2<String,Integer>,Boolean> filterFunction = w -> (w._2 == 5);

		JavaPairRDD<String,Integer> rddf = wordsPair.filter(filterFunction);
		rddf.foreach(item -> System.out.println(item));

		//DISTINCT function
		data = Arrays.asList("Learn", "Apache", "Spark", "with", "Spark","Tutorial");
		words = sc.parallelize(data, 1);

		// get distinct elements of RDD
		JavaRDD<String> rddDistinct = words.distinct();

		// print
		rddDistinct.foreach(item -> {
			System.out.println(item);
		});

		//REDUCE function (aggregation)
		// read text file to RDD
		numbers = sc.parallelize(Arrays.asList(14,21,88,99,455));

		// aggregate numbers using addition operator
		int sum = numbers.reduce((a,b)->a+b); 

		System.out.println("Sum of numbers is : "+sum);
	}
}
//Custom class
class Person implements Serializable{
	private static final long serialVersionUID = -2685444218382696366L;
	String name;
	int age;
	public Person() {}
	public Person(String name, int age){
		this.name = name;
		this.age = age;
	}
}