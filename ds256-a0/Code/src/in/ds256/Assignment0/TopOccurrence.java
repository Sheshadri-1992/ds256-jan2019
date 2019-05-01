package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for finding frequent co-occuring hash-tags
 */
public class TopOccurrence {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS

		SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputTweets = sc.textFile(inputFile);



		inputTweets = inputTweets.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String jsonString) throws Exception {
				if (jsonString.contains("\"delete\""))
					return false;

				return true;
			}
		});



		JavaPairRDD<String, Integer> coHashTag = inputTweets
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

					@Override
					public Iterator<Tuple2<String, Integer>> call(String jsonString) throws Exception {

						ArrayList<Tuple2<String, Integer>> myIter = new ArrayList<Tuple2<String, Integer>>();
						Parser myParse = new Parser();

						if (jsonString == null || jsonString.isEmpty())
							return myIter.iterator();

						myParse.setInputJson(jsonString);

						if (myParse.getHashTags() > 1) {
							int totalHashTags = myParse.getHashTags();
							ArrayList<String> hashTags = myParse.getHashTagArray();

							/** Sort the hash tags in lexicographic ascending order **/
							hashTags.sort(new Comparator<String>() {
								@Override
								public int compare(String o1, String o2) {
									int compare = o1.compareTo(o2);
									return compare;
								}
							});
							
							String pairHashTag = "";

							for (int i = 0; i < totalHashTags; i++) {
								for (int j = i + 1; j < totalHashTags; j++) {
									
									/** If both hash tags are same then skip it , don't count it**/
									if(hashTags.get(i).compareTo(hashTags.get(j))==0)
										continue;
									
									pairHashTag = hashTags.get(i) + ":" + hashTags.get(j);

									Tuple2<String, Integer> myTuple = new Tuple2<String, Integer>(pairHashTag, 1);
									myIter.add(myTuple);
								}
							}

						}

						return myIter.iterator();
					}

				});

		coHashTag = coHashTag.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		JavaPairRDD<Integer, String> sortedRDD = coHashTag
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {

						Tuple2<Integer, String> myTuple = new Tuple2<Integer, String>(t._2, t._1);
						return myTuple;
					}
				});

		
		/** Sorted in decreasing order **/
		sortedRDD = sortedRDD.sortByKey(false);

		ArrayList<Integer> myTop100Keys = new ArrayList<Integer>();
		ArrayList<String> myTop100HashTags = new ArrayList<String>();

		/** Take the top 100 counts of the co-occurring hashtags **/
		for (Integer item : sortedRDD.keys().top(100)) {
			System.out.println("The counts of the hashtags are " + item);
			myTop100Keys.add(item);
		}
		
		/** Take the top 100 strings of the co-occurring hashtags **/
		for (String pairhashTag : sortedRDD.values().top(100)) {
			System.out.println("The hashtags are " + pairhashTag);
			myTop100HashTags.add(pairhashTag);
		}

		JavaRDD<Integer> top100keyRDD = sc.parallelize(myTop100Keys);
		JavaRDD<String> top100HashTag100 = sc.parallelize(myTop100HashTags);

		top100keyRDD.coalesce(1);
		top100keyRDD.saveAsTextFile(outputFile+"/keys");

		top100HashTag100.coalesce(1);
		top100HashTag100.saveAsTextFile(outputFile+"/values");

		sc.stop();
		sc.close();
	}

}