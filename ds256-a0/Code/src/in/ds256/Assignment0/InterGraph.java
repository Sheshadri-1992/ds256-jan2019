package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.JSONObject;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for generating interaction graph
 */
public class InterGraph {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		String vertexFile = args[1]; // Should be some file on HDFS
		String edgeFile = args[2]; // Should be some file on HDFS

		SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		long startTime = Time.now();

		JavaRDD<String> inputTweets = sc.textFile(inputFile);

		System.out.println("JSR");

		/** Remove all the deleted tweets **/
		inputTweets = inputTweets.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String jsonString) throws Exception {

				if (jsonString.contains("\"delete\""))
					return false;

				return true;

			}
		});

		/** Create a RDD of users and their followers, friends **/
		JavaPairRDD<String, String> vertexRDD = inputTweets
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {

					@Override
					public Iterator<Tuple2<String, String>> call(Iterator<String> jsonFile) throws Exception {

						ArrayList<Tuple2<String, String>> myIter = new ArrayList<Tuple2<String, String>>();
						Parser myParse = new Parser();

						while (jsonFile.hasNext()) {
							String jsonString = jsonFile.next();
							myParse.setInputJson(jsonString);

							if (jsonString == null || jsonString.isEmpty())
								continue;
							String userName = myParse.getUser();

							if (userName == null || userName.isEmpty())
								continue;

							String createdAt = myParse.getCreatedAt(); // created time for the user

							/** Value part of the RDD **/
							String timeStamp = myParse.getTimeStamp();
							
							if(createdAt==null || createdAt.isEmpty() || timeStamp==null || timeStamp.isEmpty())
								continue;
							
							String followersCount = myParse.getFollowersCount().toString();
							String friendsCount = myParse.getFriendsCount().toString();

							/** key for the RDD **/
							String key = userName + "," + createdAt;
							/** Value for the RDD **/
							String value = timeStamp + "," + followersCount + "," + friendsCount;

							Tuple2<String, String> myTuple = new Tuple2<String, String>(key, value);
							myIter.add(myTuple);

						}

						return myIter.iterator();

					}
				});
		
		

		/** Reduce the User Tweets by userID **/
		vertexRDD = vertexRDD.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				String result = v1 + "," + v2;

				return result;
			}
		});

		/** Create RDD for edge **/
		JavaPairRDD<String, String> edgeRDD = inputTweets
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {

					@Override
					public Iterator<Tuple2<String, String>> call(Iterator<String> jsonFile) throws Exception {

						ArrayList<Tuple2<String, String>> myIter = new ArrayList<Tuple2<String, String>>();

						Parser sourceTweetParser = new Parser();
						Parser sinkTweetParser = new Parser();

						while (jsonFile.hasNext()) {
							String jsonString = jsonFile.next();

							if (jsonString == null || jsonString.isEmpty())
								continue;

							sourceTweetParser.setInputJson(jsonString);

							/** Check if it is a retweeted tweet **/
							JSONObject sinkTweet = sourceTweetParser.getRetweetJsonObject();
							if (sinkTweet == null)
								continue;

							//sourceID
							String sourceId = sourceTweetParser.getUser();
							if(sourceId==null || sourceId.isEmpty()) //check if source user ID ie retweeter id is empty or null
								continue;

							sinkTweetParser.setInputJsonObject(sinkTweet);

							JSONObject sinkUsrObj = sinkTweet.getJSONObject("user");
							if (sinkUsrObj == null)
								continue;

							//sinkID
							String sinkId = sinkUsrObj.getString("id_str");
							if (sinkId == null || sinkId.isEmpty())//check if source user ID ie tweeter id is empty or null
								continue;

							String timeStamp = sourceTweetParser.getCreatedAt(); //tweetjson's created at
							String tweetId = sinkTweet.getString("id_str"); //the original tweeter's tweet id, id_str in the retweeted_status JSON
							String retweetId = sourceTweetParser.getTweetId(); //the retweeters tweet id, id_str in the main JSON
							
							/** Null and exception check**/
							if(timeStamp==null || tweetId== null || retweetId==null || timeStamp.isEmpty() || tweetId.isEmpty() || retweetId.isEmpty())
								continue;

							ArrayList<String> hashTags = sinkTweetParser.getHashTagArray();

							String key = sourceId + "," + sinkId;
							String value = timeStamp + "," + tweetId + "," + retweetId;

							// TODO: Use a string builder
							for (String hashTag : hashTags) {
								value = value + "," + hashTag;
							}

							Tuple2<String, String> edgeTuple = new Tuple2<String, String>(key, value);
							myIter.add(edgeTuple);

						}

						return myIter.iterator();

					}
				});

		/** Reduce by key to aggregate the edge interactions **/
		edgeRDD = edgeRDD.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {

				String result = v1 + ";" + v2;
				return result;
			}
		});

		JavaRDD<String> result = edgeRDD.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				String result = v1._1 + ";" + v1._2;
				return result;
			}
		});

//		for (String item : vertexRDD.keys().collect()) {
//			System.out.println(" The  key is " + item);
//		}
//
//		for (String item : vertexRDD.values().collect()) {
//			System.out.println(" The  value is " + item);
//		}

		/** Save it to HDFS **/
		vertexRDD.saveAsTextFile(vertexFile);
		result.saveAsTextFile(edgeFile);

		long end = Time.now();

		System.out.println("The time taken in seconds is  " + (end - startTime) / 1000);

		sc.stop();
		sc.close();
	}

}