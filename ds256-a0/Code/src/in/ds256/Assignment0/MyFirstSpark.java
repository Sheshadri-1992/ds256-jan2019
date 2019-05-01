package in.ds256.Assignment0;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MyFirstSpark implements Serializable {

	private static final long serialVersionUID = 1L;
	public static long[] array = new long[94];

	public class UserHashtagCount implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public String userId = "";
		public long totalHashTags;
		public long totalTweets;

		public UserHashtagCount(String argUserId, long argHashtagCount, long argTotalTweet) {
			userId = argUserId;
			totalHashTags = argHashtagCount;
			totalTweets = argTotalTweet;
		}

		public void addHashTags(long hashTags) {
			totalHashTags = totalHashTags + hashTags;
		}

		public void addTweets(long tweets) {
			totalTweets = totalTweets + tweets;
		}

		public String getUser() {
			return userId;
		}

		public long getTotalHashTags() {
			return totalHashTags;
		}

		public long getTotalTweets() {
			return totalTweets;
		}
	}

	/**/
	public PairFunction<String, String, UserHashtagCount> hasTagFunction = new PairFunction<String, String, UserHashtagCount>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, UserHashtagCount> call(String jsonString) throws Exception {

			Parser myParse = new Parser(jsonString);
			String userName = myParse.getUser();
			UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags(), 1);

			return new Tuple2<String, MyFirstSpark.UserHashtagCount>(userName, userHasCount);
		}

	};

	/** This is for mapPartitionsToPair **/
	public PairFlatMapFunction<Iterator<String>, String, UserHashtagCount> myTag = new PairFlatMapFunction<Iterator<String>, String, UserHashtagCount>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String, UserHashtagCount>> call(Iterator<String> t) throws Exception {

			// TODO Auto-generated method stub
			ArrayList<Tuple2<String, UserHashtagCount>> iter = new ArrayList<Tuple2<String, UserHashtagCount>>();
			Parser myParse = new Parser();

			while (t.hasNext()) {
				String jsonObject = t.next();

				myParse.setInputJson(jsonObject);

				if (myParse.checkIfDelete()) // skip the delete sweets
					continue;

				String userName = myParse.getUser();
				UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags(), 1);

				Tuple2<String, UserHashtagCount> myTuple = new Tuple2<String, MyFirstSpark.UserHashtagCount>(userName,
						userHasCount);
				iter.add(myTuple);

			}

			return iter.iterator();
		}
	};

	/** mapToPair **/
	PairFunction<String, String, UserHashtagCount> pairTag = new PairFunction<String, String, UserHashtagCount>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, UserHashtagCount> call(String jsonObject) throws Exception {

			Parser myParse = new Parser();

			myParse.setInputJson(jsonObject);

			if (myParse.checkIfDelete()) // skip the delete sweets
				return null;

			String userName = myParse.getUser();
			UserHashtagCount userHasCount = new UserHashtagCount(userName, myParse.getHashTags(), 1);

			Tuple2<String, UserHashtagCount> myTuple = new Tuple2<String, MyFirstSpark.UserHashtagCount>(userName,
					userHasCount);

			return myTuple;
		}
	};

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf().setMaster("local").setAppName("MyFirstSpark");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);

		long startTime = Time.now();
		// this is creation of a RDD
		JavaRDD<String> linesRDD = sc.parallelize(
				Arrays.asList("Krishna", "Rama", "Krishna was from dwapara yuga", "Rama was from treta yuga"));

		// Lets look at a transformation, lines is a RDD, we applied transformation to
		// it to get krishnaRDD
		JavaRDD<String> krishnaRDD = linesRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("Krishna");

			}
		});

		// similarly here we applied transformation to get ramaRDD
		JavaRDD<String> ramaRDD = linesRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("Rama");

			}
		});

		// UnionRDD , operates on 2 RDDs, it is also a type of transformation since it
		// returns a RDD
		JavaRDD<String> unionRDD = krishnaRDD.union(ramaRDD);

		// count() is an action
		System.out.println("The unionRDD has " + unionRDD.count() + " count"); /// expectation is 4
		System.out.println("Here are the items in the unionRDD");

		for (String item : unionRDD.take(4)) { // take is an action
			System.out.println("Here is the element " + item);
		}

		// RDD of Integers
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> squareRDD = intRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer x) throws Exception {
				// TODO Auto-generated method stub
				return x * x;
			}

		});

		// collect() returns all the items of the RDD< use this if the result is small.
		System.out.println("The result is " + squareRDD.collect().toString());

		/** Important stuff starts here **/

		MyFirstSpark mySparkObj = new MyFirstSpark();

		JavaRDD<String> jsonPairRDD = sc.textFile(Constants.TWEET_DIR_PATH);

		/* Placeholder to see sample data from 1TB */

		long end = Time.now();

		System.out.println("Filter operation The time taken in seconds is  " + (end - startTime) / 1000);

		/** Most crucial function **/
		JavaPairRDD<String, UserHashtagCount> userHashCountRDD = jsonPairRDD.mapPartitionsToPair(mySparkObj.myTag);

//		JavaPairRDD<String, UserHashtagCount> userHashCountRDD = jsonPairRDD.mapToPair(mySparkObj.pairTag);

		System.out.println("The number of partions of userHashCountRDD are " + userHashCountRDD.getNumPartitions());

		end = Time.now();
		System.out.println("The time taken in seconds is  after map partions to pair " + (end - startTime) / 1000);

		/** Aggregating the things **/

		JavaPairRDD<String, UserHashtagCount> groupedHashCount = userHashCountRDD.reduceByKey(
				new Function2<MyFirstSpark.UserHashtagCount, MyFirstSpark.UserHashtagCount, MyFirstSpark.UserHashtagCount>() {

					private static final long serialVersionUID = 1L;

					@Override
					public UserHashtagCount call(UserHashtagCount v1, UserHashtagCount v2) throws Exception {
						v1.addHashTags(v2.getTotalHashTags());
						v1.addTweets(v2.getTotalTweets());

						return v1;
					}
				});
		
		groupedHashCount.cache();

		/** Printing to check the results **/
		for (UserHashtagCount item : groupedHashCount.values().take(5)) {

			Double hashPerTweet = (double) ((double) item.getTotalHashTags() / (double) item.getTotalTweets());
			System.out.println("The user is " + item.getUser() + " total Tweets are " + item.getTotalTweets()
					+ " hashtags are " + item.getTotalHashTags() + " ratio is " + hashPerTweet);
		}

		JavaRDD<Double> countRDD = groupedHashCount.map(new Function<Tuple2<String, UserHashtagCount>, Double>() {

			@Override
			public Double call(Tuple2<String, UserHashtagCount> tuple) throws Exception {
				// TODO Auto-generated method stub

				UserHashtagCount userHashRatio = tuple._2;
				Double hashPerTweet = (double) ((double) userHashRatio.getTotalHashTags()
						/ (double) userHashRatio.getTotalTweets());

				int bucket = hashPerTweet.intValue(); //eg 0.14 will be 0, 
//				System.out.println("The bucket value is " + bucket);
				array[bucket] = array[bucket] + 1;
				return hashPerTweet;
			}
		});

		countRDD.count();
		

		ArrayList<Long> tweetBuckets = new ArrayList<Long>();
		for (int i = 0; i < 94; i++) {
			System.out.println(" bucket " + i + " size is " + array[i]);
			tweetBuckets.add(array[i]);

		}
		
		JavaRDD<Long> bucketRDD = sc.parallelize(tweetBuckets);
//		bucketRDD.coalesce(1).saveAsTextFile(Constants.TWEET_OUTPUT_DIR);
		
		/** Create a RDD of <String,String> the key String is user_ID,created_at, [,timestamp,followers_count **/
		JavaRDD<String> testing = sc.parallelize(Arrays.asList("Rama","Krishna"));
		JavaPairRDD<String,String> testingPair = testing.mapToPair( new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				
				System.out.println("The string is "+t);
				
				String key = "";
				String value = "";
				
				
				if(t.compareTo("Rama")==0)
					key="Rama";
				else
					key="Krishna";
				
				value = "god";
				
				
				Tuple2<String, String> myTuple = new Tuple2<String, String>(key,value);
				return myTuple;

			}
		});
		 	
		
		for(Long count: bucketRDD.collect()) {
			System.out.println(count);
		}

		end = Time.now();

		System.out.println("The time taken in seconds is  " + (end - startTime) / 1000);
		System.out.println("");


		System.out.println("This should be over");
		sc.stop();
		sc.close();
	}

}
