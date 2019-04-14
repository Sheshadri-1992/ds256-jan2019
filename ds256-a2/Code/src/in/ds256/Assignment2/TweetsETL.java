package in.ds256.Assignment2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class TweetsETL {

	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n" + "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}

		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		System.out.println("Thee broker is " + broker + " the topic is " + topic);
		System.out.println("The output file is " + output);

		// Create context with a 10 seconds batch interval
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("TweetETL");

//		SparkConf sparkConf = new SparkConf().setAppName("TweetETL");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		JavaDStream<String> tweets = messages.map(ConsumerRecord::value);
		tweets = tweets.repartition(4);

		/** delete all the tweets which contain delete in them **/
		tweets = tweets.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {

				if (arg0.contains("\"delete\""))
					return false;

				return true;
			}
		});

		System.out.println("The number of tweets are " + tweets.count());

		JavaDStream<String> csvFormat = tweets.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			String[] stopWords = { "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
					"are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
					"but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
					"doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has",
					"hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
					"hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
					"in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most",
					"mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other",
					"ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd",
					"she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the",
					"their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd",
					"they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up",
					"very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
					"what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why",
					"why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
					"yours", "yourself", "yourselves" };
			
			/**
			 * 
			 * @param string : the actual tweet
			 * @return tweet after removing 
			 */
			public String removeWord(String string) 
		    { 
		  
				for(int i=0;i<stopWords.length;i++) {
				
					if (string.contains(stopWords[i])) { 
						  
			            String tempWord = stopWords[i] + " "; 			            
			            tempWord = "\\b" + stopWords[i]+ "\\b";
			            System.out.println("The word is "+tempWord);
			            string = string.replaceAll(tempWord, "");
			        }	
				}
				
		        return string; 
		    }

			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {

				ArrayList<String> myString = new ArrayList<String>();

				Parser myParser = new Parser();

				while (arg0.hasNext()) {
					String line = arg0.next();

					String csv = "";

					if (line == null || line.isEmpty())
						continue;

					if (!myParser.isJSONValid(line))
						continue;

					myParser.setInputJson(line);

					String epoch_time = myParser.getTimeStamp();
					String time_zone = myParser.getTimeZone();
					String lang = myParser.getLanguage();
					String tweetId = myParser.getTweetId();
					String userId = myParser.getUser();
					Integer followersCount = myParser.getFollowersCount();
					Integer friendsCount = myParser.getFriendsCount();
					String tweet = myParser.getTweet().replace(",", "").toLowerCase(); /* Lowercase */
					ArrayList<String> hashTagArray = myParser.getHashTagArray();

					if (epoch_time == null || epoch_time.isEmpty() || tweetId == null || tweetId.isEmpty()
							|| userId == null || userId.isEmpty())
						continue;

					StringBuilder hashTags = new StringBuilder();

					if (hashTagArray.size() > 0) {
						int i = 0;
						for (i = 0; i < hashTagArray.size() - 1; i++) {
							hashTags.append(hashTagArray.get(i).toLowerCase() + ";");
						}

						hashTags.append(hashTagArray.get(i).toLowerCase());
					}

					csv = epoch_time + "," + time_zone + "," + lang + "," + tweetId + "," + userId + ","
							+ followersCount + "," + friendsCount + "," + tweet + "," + hashTags;
					System.out.println("The csv is " + csv);
					myString.add(csv);

				}

				return myString.iterator();
			}
		});

//		JavaDStream<String> csvFormat = tweets.map(new Function<String, String>() {
//
//			@Override
//			public String call(String arg0) {
//
//				String csv = "";
//				
//				if(arg0==null || arg0.isEmpty())
//					return null;
//				
//				Parser myParser = new Parser(arg0);
//				if(!myParser.isJSONValid(arg0))
//					return null;
//				
//				String epoch_time = myParser.getTimeStamp();
//				String time_zone = myParser.getTimeZone();
//				String lang = myParser.getLanguage();
//				String tweetId = myParser.getTweetId();
//				String userId = myParser.getUser();
//				Integer followersCount = myParser.getFollowersCount();
//				Integer friendsCount = myParser.getFriendsCount();
//				String tweet = myParser.getTweet().replace(",", "").toLowerCase(); /* Lowercase*/
//				ArrayList<String> hashTagArray = myParser.getHashTagArray();
//
//				StringBuilder hashTags = new StringBuilder();
//
//				if (hashTagArray.size() > 0) {
//					int i = 0;
//					for (i = 0; i < hashTagArray.size() - 1; i++) {
//						hashTags.append(hashTagArray.get(i) + ";");
//					}
//
//					hashTags.append(hashTagArray.get(i));
//				}
//
//				csv = epoch_time + "," + time_zone + "," + lang + "," + tweetId + "," + userId + "," + followersCount
//						+ "," + friendsCount + "," + tweet + "," + hashTags;
//				System.out.println("The csv is " + csv);
//
//				return csv;
//			}
//		});
		csvFormat.print();
		csvFormat.dstream().saveAsTextFiles(output, "");

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}