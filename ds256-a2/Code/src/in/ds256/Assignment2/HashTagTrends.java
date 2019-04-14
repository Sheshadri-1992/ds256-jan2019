package in.ds256.Assignment2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class HashTagTrends {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n"
					+ "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}
	
		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		// Create context with a 10 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("HashTagTrends");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/**
		*	Code goes here....
		*/
		JavaDStream<String> csvFormat = messages.map(ConsumerRecord::value);
		csvFormat = csvFormat.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String arg0) throws Exception {
				
				if(arg0==null || arg0.isEmpty()) {
					return false;
				}
				else {
					char firstChar = arg0.charAt(0);
					System.out.println("The arg0 is ");
					return Character.isDigit(firstChar);
				}			
					
			}
		});

		JavaPairDStream<String, Integer> hashCounts1 = csvFormat.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(Iterator<String> arg0) throws Exception {
				
				ArrayList<Tuple2<String,Integer>> myIter = new ArrayList<>();
						
				while(arg0.hasNext()) {
					String csvLine = arg0.next();
					
					String[] csvArray = csvLine.split(",");
					String hashTags = csvArray[csvArray.length-1];
					
					String[] hashTagsArray = hashTags.split(";");
					
					for(String hashTag : hashTagsArray) {
						Tuple2<String,Integer> myTuple = new Tuple2<String, Integer>(hashTags, 1);
						myIter.add(myTuple);
					}
				}
				
				return myIter.iterator();
			}
		});
		
		hashCounts1 = hashCounts1.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		}, new Duration(5000)); /** This 5000 milli seconds is they key here **/
		
		JavaPairDStream<Integer, String> topHashTags = hashCounts1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,Integer>>, Integer, String>() {

			@Override
			public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Integer>> arg0) throws Exception {
				ArrayList<Tuple2<Integer,String>> myIter = new ArrayList<>();
				
				while(arg0.hasNext()) {
					Tuple2<String,Integer> hashTagTuple = arg0.next();
					Tuple2<Integer, String> myTuple = new Tuple2<Integer, String>(hashTagTuple._2(), hashTagTuple._1());
					myIter.add(myTuple);
				}
				
				return myIter.iterator();
			}
		});
		
		topHashTags = topHashTags.transformToPair(new Function<JavaPairRDD<Integer,String>, JavaPairRDD<Integer,String>>() {

			@Override
			public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> arg0) throws Exception {

				return arg0.sortByKey(false);
				
			}
		});
		
		System.out.println("The top 5 hashtags are "+"");
		
		topHashTags.print(5);
				
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
