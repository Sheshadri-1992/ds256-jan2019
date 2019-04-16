package in.ds256.Assignment2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

public class SentimentAnalysis {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n" + "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}

		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		// Create context with a 10 seconds batch interval
//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("UserCount");
		SparkConf sparkConf = new SparkConf().setAppName("UserCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(180));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/**
		 * Code goes here....
		 */

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

		JavaPairDStream<String, Long> sentimentTweets = tweets
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Long>() {
					
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

					String[] negativeAffect = { "afraid", "scared", "nervous", "jittery", "irritable", "hostile",
							"guilty", "ashamed", "upset", "distressed" };
					
					String[] positiveAffect = { "active", "alert", "attentive", "determined", "enthusiastic", "excited",
							"inspired", "interested", "pround", "strong" };
					
					String[] fear = { "afraid", "scared", "frightened", "nervous", "jittery", "shaky" };
					
					String[] hostility = { "angry", "hostile", "irritable", "scornful", "disgusted", "loathing" };
					
					String[] guilty = { "guilty", "ashamed", "blameworthy", "angry at self", "disgusted with self",
							"dissatisfied with self" };
					
					String[] sadness = { "sad", "blue", "downhearted", "alone", "lonely" };
					
					String[] joviality = { "happy", "joyful", "delighted", "cheerful", "excited", "enthusiastic",
							"lively", "energetic" };
					
					String[] selfAssurance = { "proud", "strong", "confident", "bold", "daring", "fearless" };
					
					String[] attentiveness = { "alert", "attentiveness", "concentrating", "determined" };
					
					String[] shyness = { "shy", "bashful", "sheepish", "timid" };
					
					String[] fatigue = { "sleepy", "tired", "sluggish", "drowsy" };					
					
					String[] serenity = { "calm", "relaxed", "at ease" };
					
					String[] surprise = { "amazed", "surprised", "astonished" };

					String[][] sentiments = { negativeAffect, positiveAffect, fear, hostility, guilty, sadness,
							joviality, selfAssurance, attentiveness, shyness, fatigue, serenity, surprise };

					public int returnSentiment(String tweet, ArrayList<String> hashTagsArray) {

						boolean found = false;
						int sentiment = -1;
						int wordpos = -1;

						if (tweet == null)
							return -1;

						for (int i = 0; i < sentiments.length; i++) {

							for (int j = 0; j < sentiments[i].length; j++) {

								if (tweet.contains(sentiments[i][j])) {
									found = true;
									sentiment = i; /** This is important **/
									wordpos = j; /** Word **/
									break;
								}
							}

							if (found)
								break;
						}
						
						if(sentiment!=-1 && wordpos!=-1)
							System.out.println("The sentiment is " + sentiment + " The position is " + wordpos
								+ " The word is " + sentiments[sentiment][wordpos]);
						return sentiment;
					}
					
					/**
					 * 
					 * @param string : the actual tweet
					 * @return tweet after removing 
					 */
					public String removeWord(String string) 
				    { 
				  
						/** Iteratre over all the stopwords **/
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
					public Iterator<Tuple2<String, Long>> call(Iterator<String> arg0) throws Exception {

						ArrayList<Tuple2<String, Long>> myIter = new ArrayList<Tuple2<String, Long>>();
						Parser myParser = new Parser();

						while (arg0.hasNext()) {

							String emotion = "";
							String line = arg0.next();

							if (line == null || line.isEmpty())
								continue;

							if (!myParser.isJSONValid(line))
								continue;

							myParser.setInputJson(line);
							String tweet = myParser.getTweet().replace(",", "").toLowerCase(); /* Lowercase */
							
							/** Remove stop words **/
							if(tweet!=null)
								tweet=removeWord(tweet);
							
							/** Remove punctuation **/
							tweet = tweet.replaceAll("'", ""); 
							
							/** Remove new line characters **/
							tweet = tweet.replaceAll("\n","");
							
							ArrayList<String> hashTagArray = myParser.getHashTagArray();

							int sentiment = returnSentiment(tweet, hashTagArray);

							switch (sentiment) {
							
								case -1:
									System.out.println("No emotion ");
									emotion = "unknown emotion";
									break;
								case 0:
									emotion = "negativeAffect";
									System.out.println("Emotion is "+emotion);
									break;
								case 1:
									emotion = "positiveAffect";
									System.out.println("Emotion is "+emotion);
									break;
								case 2:
									emotion = "fear";
									System.out.println("Emotion is "+emotion);
									break;
								case 3:
									emotion = "hostility";
									System.out.println("Emotion is "+emotion);
									break;
								case 4:
									emotion = "guilty";
									System.out.println("Emotion is "+emotion);
									break;
								case 5:
									emotion = "sadness";
									System.out.println("Emotion is "+emotion);
									break;
								case 6:
									emotion = "joviality";
									System.out.println("Emotion is "+emotion);
									break;
								case 7:
									emotion = "selfAssurance";
									System.out.println("Emotion is "+emotion);
									break;
								case 8:
									emotion = "attentiveness";
									System.out.println("Emotion is "+emotion);
									break;
								case 9:
									emotion = "shyness";
									System.out.println("Emotion is "+emotion);
									break;
								case 10:
									emotion = "fatigue";
									System.out.println("Emotion is "+emotion);
									break;
								case 11:
									emotion = "serenity";
									System.out.println("Emotion is "+emotion);
									break;
								case 12:
									emotion = "surprise";
									System.out.println("Emotion is "+emotion);
									break;
								default:
									System.out.println("Unknown option");
									break;

							} /** End of switch case **/
							
						
						Tuple2<String,Long> myTuple = new Tuple2<String, Long>(emotion, 1L);
						myIter.add(myTuple);	
							
						}/** End of while loop **/
						
						

						return myIter.iterator();
					}
				});
		
		sentimentTweets = sentimentTweets.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
			
			@Override
			public Long call(Long arg0, Long arg1) throws Exception {

				return arg0+arg1; /** This is important **/
			}
		},new Duration(180000), new Duration(180000));
		
		JavaPairDStream<Long, String> countEmotionsDStream = sentimentTweets.mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Long> arg0) throws Exception {
				
				return new Tuple2<Long,String>(arg0._2(),arg0._1()); /** so that we can sort based on counts **/

			}
		});
		
		/** Note that the key is count **/
		countEmotionsDStream = countEmotionsDStream.transformToPair(new Function<JavaPairRDD<Long,String>, JavaPairRDD<Long,String>>() {

			@Override
			public JavaPairRDD<Long, String> call(JavaPairRDD<Long, String> arg0) throws Exception {

				return arg0.sortByKey(false);
			}
		});

		countEmotionsDStream.print(3);
		countEmotionsDStream.dstream().saveAsTextFiles(output, "");
		
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
