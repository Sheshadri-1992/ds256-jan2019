package in.ds256.Assignment2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
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

/** A2 **/
public class UserCount {

	/** This is to maintain trailing zeroes **/
	private static final double PHI = 0.77351D;

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
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

		/**
		 * https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/total.html
		 */
//		jssc.checkpoint("/user/sheshadrik/checkpoint/");

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		JavaDStream<String> csvFormat = messages.map(ConsumerRecord::value);
		csvFormat = csvFormat.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {

				if (arg0 == null || arg0.isEmpty()) {
					return false;
				} else {
					char firstChar = arg0.charAt(0);
					return Character.isDigit(firstChar);
				}

			}
		});

		JavaPairDStream<Long, String> userPairRDD = csvFormat
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, String>() {

					@Override
					public Iterator<Tuple2<Long, String>> call(Iterator<String> arg0) throws Exception {

						ArrayList<Tuple2<Long, String>> myIter = new ArrayList<Tuple2<Long, String>>();

						while (arg0.hasNext()) {
							String line = arg0.next();

							String userId = null;

							try {
								userId = line.split(",")[4]; /* Index 5 is the position for userId **/
							} catch (Exception e) {
								System.out.println("User Id wasn't present, mishandlded csv");
								continue;
								
							}

							myIter.add(new Tuple2<Long, String>(1L, userId));
						}

						return myIter.iterator();

					}
				});

		JavaPairDStream<Long, Long> hashCodeRDD = userPairRDD
				.mapToPair(new PairFunction<Tuple2<Long, String>, Long, Long>() {

					/**
					 * 
					 * @param hashCode
					 * @return
					 */
					public int returnBitPosition(int hashCode) {
						int pos = 0;

						for (int i = 0; i < 32; i++) {
							if ((hashCode % 2) == 0) {
								hashCode = hashCode / 2;
								pos++;
							} else {
								break;
							}
						}
						if (pos == 32 || pos == 0)
							pos = 0;

						System.out.println("The position is " + pos);

						return pos;

					}

					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, String> arg0) throws Exception {

						Tuple2<Long, Long> myIter = null;

						String line = arg0._2();
						int hashCode = (line.hashCode()) % 100000;
						int pos = returnBitPosition(hashCode);

						Long newState = 0L;

						newState = newState + (long) Math.pow(2, pos); /** This is imp **/

						myIter = new Tuple2<Long, Long>(1L, (long) pos); /** Most important step **/

						return myIter;
					}
				});

		JavaPairDStream<Long, Long> reduceHashCode = hashCodeRDD
				.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long arg0, Long arg1) throws Exception {
						return Math.max(arg0, arg1);
					}
				}, new Duration(60000), new Duration(60000));/** Every 60 seconds **/

		JavaPairDStream<Long, Long> resultRDD = reduceHashCode
				.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, Long> arg0) throws Exception {

						Long oredHashCode = arg0._2();
//				String finalBinary = Long.toBinaryString(oredHashCode);

						int pos = oredHashCode.intValue();

//				for(int i=0;i<32;i++) {
//					if((oredHashCode % 2)==0) {							
//						pos++;
//					}
//					else {
//						break;
//					}
//				}

						Long result = 0L;
						result = (long) (Math.pow(2, pos) / PHI);

						System.out.println("The result is " + result);

						return new Tuple2<Long, Long>(1L, result);
					}
				});

		/** This is needed for check-pointing page 186 spark programming **/
//		jssc.checkpoint("/user/sheshadrik/checkpoint");

		resultRDD = resultRDD.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

			@Override
			public Optional<Long> call(List<Long> values, Optional<Long> state) throws Exception {

				Long totalSum = state.or(0L);

				for (Long val : values) {
					totalSum = totalSum + val;
				}

				return Optional.of(totalSum);
			}
		});
		jssc.checkpoint("file:///home/sheshadrik/q2aCheckpoint");

		resultRDD.print();
		resultRDD.dstream().saveAsTextFiles(output + "q2b", "");

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}