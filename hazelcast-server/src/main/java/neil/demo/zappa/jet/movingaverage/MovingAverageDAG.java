package neil.demo.zappa.jet.movingaverage;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedFunctions;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.kafka.KafkaProcessors;
import neil.demo.zappa.CurrencyPairKey;
import neil.demo.zappa.CurrencyPairValue;
import neil.demo.zappa.MyConstants;
import neil.demo.zappa.TimePrice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * <p>Builder class to construct a <I><b>D</b>irected <b>A</b>cyclic <b>G</b>raph</i>.
 * This is run as a job within Jet on demand.
 * </p>
 * <p>In this case the job is reading from a price source and calculating three
 * variations of moving average analysis on it.
 * </p>
 * <ol>
 * <li>The moving average of the last 200 points</li>
 * <li>The moving average of the last 50 points</li>
 * <li>The moving average of the last 1 points</li>
 * </ol>
 * <p>The latter case, the average of "1" is just another way to load
 * the latest value.
 * </p>
 */
public class MovingAverageDAG {

    private static final int ONE_DAY_IN_MS = 24 * 60 * 60 * 1000;
    private static final int TWO_DAYS_IN_MS = 2 * ONE_DAY_IN_MS;

    private static final DistributedFunction
    	<ConsumerRecord<CurrencyPairKey, CurrencyPairValue>,
    	ConsumerRecord<CurrencyPairKey, CurrencyPairValue>>
    	NO_KAFKA_PROJECTION_FILTER = DistributedFunctions.wholeItem();

    private static String MY_KAFKA_KEY_DESERIALIZER_CLASS_NAME = 
    		CurrencyPairKey.class.getCanonicalName()
			 + "$" + CurrencyPairKey.CurrencyPairKeyDeserializer.class.getSimpleName();
    private static String MY_KAFKA_VALUE_DESERIALIZER_CLASS_NAME = 
    		CurrencyPairValue.class.getCanonicalName()
			 + "$" + CurrencyPairValue.CurrencyPairValueDeserializer.class.getSimpleName();
    
	/**
	 * <p>From top to bottom, the DAG graph can be visualised
	 * as below:
	 * <pre>
	 *                      +------------+
	 *                      |1  Source   |
	 *                      | "currency" |
	 *                      |    topic   |
	 *                      +------------+
	 *                     /       |      \
	 *                    /        |       \
	 *                   /         |        \
	 *                  /          |         \
	 *    +------------+    +------------+    +------------+     
	 *    |2  Moving   |    |3  Moving   |    |4  Moving   |
	 *    |  Average   |    |  Average   |    |  Average   |
	 *    |   Of 1     |    |   Of 50    |    |   Of 200   |
	 *    +------------+    +------------+    +------------+     
	 *          |       \          |      \  /       |
	 *          |        \         |       \/        |
	 *          |         \        |       /\        |
	 *          |          \       |      /  \       |
	 *    +------------+    +------------+    +------------+     
	 *    |5   Sink    |    |6   Price   |    |7   Co-     |
	 *    |     To     |    |  Formatter |    |   Grouper  |
	 *    | SystemOut  |    |            |    |            |
	 *    +------------+    +------------+    +------------+    
	 *                             |                 |
	 *                             |                 |
	 *                             |                 |
	 *                             |                 |
	 *                      +------------+    +------------+      
	 *                      |8 Sink To   |    |9  Group    |
	 *                      | "BTC/USD"  |    |  Filter    |
	 *                      |     map    |    |            |
	 *                      +------------+    +------------+     
	 *                                               |
	 *                                               |
	 *                                               |
	 *                                               |
	 *                                        +------------+      
	 *                                        |10  Cross   |
	 *                                        |  Detector  |
	 *                                        |            |
	 *                                        +------------+     
	 *                                               |      \
	 *                                               |       \
	 *                                               |        \
	 *                                               |         \
	 *                                        +------------+    +------------+      
	 *                                        |11 Sink To  |    |12 Sink To  |
	 *                                        |   "alert"  |    |  "alert"   |
	 *                                        |    topic   |    |    map     |
	 *                                        +------------+    +------------+     
	 * </pre>
	 * <p>The numbered boxes represent the vertices of the graph, and the
	 * lines the edges between them for data flow.
	 * </p>
	 * <ul>
	 * <li><b>1. Source Kafka</b>
	 * <p>This vertex is a Kafka Processor, reading continuously from
	 * a Kafka topic.</p>
	 * <p>Jet provides a Kafka reader so we use this rather than
	 * write out own. Kafka needs connection properties, and
	 * deserializers to turn the topic data into Java entries.</p>
	 * <p>Each data record read in is sent once each in parallel to
	 * vertices 2, 3 and 4.</p>
	 * <p>It doesn't receive any data from other vertices, it is the
	 * start of the graph so the "<i>source</i>" of data.
	 * </p>
	 * </li>
	 * <li><b>2. Average of 1</b>
	 * <p>This vertex is an instance of the {@link MovingAverageProcessor}
	 * class, configured with an averaging window of 1.</p>
	 * <p>This vertex calculates the average of each single item of
	 * input to produce a single item of output.</p>
	 * <p>So "{@code outputItem == inputItem / 1}".</p>
	 * <p>Mathematically, this is a {@code no-op}. But we do it this
	 * way to get an output feed of un-averaged prices in the exact
	 * same format as the output feed of averaged prices.
	 * </p>
	 * </li>
	 * <li><b>3. Average of 50</b>
	 * <p>This vertex is an instance of the {@link MovingAverageProcessor}
	 * class, configured with an averaging window of 50.</p>
	 * <p>When the 50th item of input has been received, the first
	 * item of output is produced, the average of days 1 to 50.</p>
	 * <p>When the 51st item of input has been received, the second
	 * item of output is produced, the average of days 2 to 51.</p>
	 * <p>When the 52nd item of input has been received, the third
	 * item of output is produced, the average of days 3 to 52.</p>
	 * </li>
	 * <li><b>4. Average of 200</b>
	 * <p>This vertex is an instance of the {@link MovingAverageProcessor}
	 * class, configured with an averaging window of 200.</p>
	 * <p>The averaging calculation is the same of course, but it won't
	 * produce any output until 200 input items are read.</p>
	 * </li>
	 * <li><b>5. Sink Logger</b>
	 * <p>This job takes an input feed from vertex 2, and uses
	 * one of the provided convenience classes to log the input
	 * to the screen.</p>
	 * <p>It doesn't send any output on to another vertex, so
	 * is a "<i>sink</i>". This is useful for debugging to see
	 * some of the data passing through the system.
	 * </p>
	 * <p>As an alternative we could write this vertex as an
	 * intermediate processor rather than a terminal processor,
	 * logging the input to the screen and passing on as output
	 * to the next stage.
	 * </p>
	 * </li>
	 * <li><b>6. Price Formatter</b>
	 * <p>Earlier vertices produce three streams of prices ; the
	 * current price, the average of the last 50 and the average
	 * of the last 200.
	 * </p>
	 * <p>This stage reads from all three of these streams,
	 * and reformats the output slightly, suitable for saving
	 * in an {@link java.util.Map} (that will actually be a
	 * {@link com.hazelcast.core.IMap}.
	 * </p>
	 * <li><b>7. Co-Grouper</b>
	 * <p>This vertex does a join on two infinite streams,
	 * as it is only fed a <b>*</b><i>window</i><b>*</b>
	 * of data from each stream.
     * </p>
	 * <p>So what it gets in are a few points in the same time
	 * range from ordinal 0 (the 50 point average) and ordinal 1
	 * (the 200 point average) and produces a single data object
	 * holding the combination of these, to make life easier
	 * for later stages.
     * </p>
	 * <p>Remember these are sliding windows on an infinite
	 * stream. You have to place windows over the stream as
	 * you can't wait for it to end (it's infinite!).
	 * </p>
	 * <p>The windows here are time based. The same data
	 * may appear in more than one window. Here the window
	 * spans two days and advances by one day. So a 
	 * window will contain Monday and Tuesday data. The next
	 * window will contain Tuesday and Wednesday, and the
	 * Tuesday in this window is the same as the Tuesday
	 * in the previous window.
     * </p>
	 * <p>Data in the window may be incomplete. The 50 point
	 * moving average will start producing data after 50
	 * days, the 200 point after 200 days. So in the window
	 * covering days 61 and 62 there will input on ordinal 0
	 * and not on ordinal 1, so this input is incomplete
	 * and so on these days the output is incomplete.
	 * Vertex 9 to the rescue here.
     * </p>
     * </li>
	 * <li><b>8. Sink To "{@code BTC/USD}" Map</b>
	 * <p>This takes the input from the price combiner vertex, and
	 * sends the output to the {@link com.hazelcast.core.IMap IMap}"
	 * named "{@code Price}".</p>
	 * <p>This is a "<i>sink</i>" as the data that goes into thie
	 * graph vertex doesn't go onwards through another edge.
	 * </p>
	 * <p>This uses a simple pre-built processor to do the saving
	 * by <u>replacing</u> any entry that is already there with
	 * the value that has come in from the edge as input. We don't
	 * need to <u>merge</u> or in any way augment the value from
	 * the Jet job into the value already present in the map,
	 * although this can be done too if needed.
	 * </p>
	 * <li><b>9. Group Filter</b>
	 * <p>The previous vertex, the {@code Co-Grouper}, can
	 * produce incomplete output (if it has incomplete input!).
	 * We don't want these for the next stage, so filter them
	 * out.
     * </p>
     * <p>This could be coded using a {@link com.hazelcast.jet.core.Processor Processor}
     * but here just embewd it as a lambda for ease.
     * </p>
     * </li>
	 * <li><b>10. Cross Detector</b>
	 * <p>This is the key vertex as far as this demo goes,
	 * it's the one that actually detects crosses in the moving
	 * averages.</p>
	 * <p>As it gets in a feed of 50 point and 200 point moving
	 * averages the logic is pretty simple. If they cross, produce
	 * an alert.</p>
	 * <p>If the 50 point is heading upwards and the 200 points
	 * is headed downwards, or the other way round, then they
	 * can cross. See the {@link CrossDetectorProcessor} class
	 * for details of what this <b>*</b>might<b>*</b> suggest.
	 * </p>
	 * </li>
	 * <li><b>11. Sink To "{@code alert}" Topic</b>
	 * <p>Potentially the {@code Cross Detector} may find
	 * a cross of the direction that the 50 point and 200 point
	 * moving averages are headed, and if so it produces a
	 * message.</p>
	 * <p>This message takes that message, and publishes it
	 * to a {@link com.hazelcast.core.ITopic ITopic}. If
	 * anyone is subscribed to the topic, they get the
	 * published message and can do with it what they
	 * feel appropriate.
	 * </p>
	 * </li>
	 * <li><b>12. Sink To "{@code alert}" Map</b>
	 * <p>The previous vertex publishes the alert to
	 * to a {@link com.hazelcast.core.ITopic ITopic}
	 * for real-time reactive purposes. This vertex
	 * is the counterpart, publishing to a
	 * {@link com.hazelcast.core.IMap IMap}
	 * </p>
	 * <p>The idea here is that the {@link com.hazelcast.core.IMap IMap}
	 * keeps a historical record of the generated
	 * event, but since you can event listeners on
	 * an {@link com.hazelcast.core.IMap IMap} you could
	 * do reactive processing here too.
	 * </ul>
	 * 
	 * @param bootstrapServers Kafka connection info
	 * @return A DAG to run as a Jet job
	 */
	public static DAG build(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MY_KAFKA_KEY_DESERIALIZER_CLASS_NAME);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MY_KAFKA_VALUE_DESERIALIZER_CLASS_NAME);
        
        DAG dag = new DAG();
        
        // Use a built-in processor to read from Kafka
        Vertex kafka = dag.newVertex("Kafka Source",
                KafkaProcessors.streamKafkaP(properties, 
                		NO_KAFKA_PROJECTION_FILTER,
                		WatermarkGenerationParams.noWatermarks(), 
                		MyConstants.KAFKA_TOPIC_NAME_FX)
                )
                ;

        // Create three processors for moving averages
        Vertex averageOf1 = dag.newVertex("Average of 1", () -> new MovingAverageProcessor(1));
        Vertex averageOf50 = dag.newVertex("Average of 50", () -> new MovingAverageProcessor(50));
        Vertex averageOf200 = dag.newVertex("Average of 200", () -> new MovingAverageProcessor(200));

        // Feed the output from Kafka into the average calculators
        dag.edge(Edge.from(kafka,0).to(averageOf1)
        		.partitioned(MovingAverageDAG.currencyPairKey()));
        dag.edge(Edge.from(kafka,1).to(averageOf50)
        		.partitioned(MovingAverageDAG.currencyPairKey()));
        dag.edge(Edge.from(kafka,2).to(averageOf200)
        		.partitioned(MovingAverageDAG.currencyPairKey()));
        
        // Print the averageOf1 (the current price) to the screen
        Vertex logSink = dag.newVertex("Log Sink", 
        		DiagnosticProcessors.writeLoggerP(item -> new String("*** " + item + " ***")));
        dag.edge(Edge.from(averageOf1,0).to(logSink));
        
        // Reformat the prices
        Vertex priceFormatter = dag.newVertex("Price Combiner", PriceFormatterProcessor::new);
        dag.edge(Edge.from(averageOf1,1).to(priceFormatter,0));
        dag.edge(Edge.from(averageOf50,0).to(priceFormatter,1));
        dag.edge(Edge.from(averageOf200,0).to(priceFormatter,2));
        
        // Save prices to an IMap
        Vertex priceSink = dag.newVertex("BTC/USD IMap Sink",
        		SinkProcessors.writeMapP(MyConstants.IMAP_NAME_BTC_USD)
        		);
        dag.edge(Edge.between(priceFormatter,priceSink));
        
        /* Defines two days of data in a window, advancing one day at a time.
         * So output is (Monday, Tuesday), (Tuesday, Wednesday), (Wednesday, Thursday)
         * as each day's average features in two windows
         */
        SlidingWindowPolicy slidingWindowPolicy = 
        		SlidingWindowPolicy.slidingWinPolicy(TWO_DAYS_IN_MS, ONE_DAY_IN_MS);
		
        // Inject timestamps into the 50-point stream so can group on sliding windows
        Vertex watermarkedAverageOf50 = dag.newVertex("Watermark 50 point Average",
        		Processors.insertWatermarksP(
        				WatermarkGenerationParams.wmGenParams(
        						TimePrice::getTimestamp, 
        						WatermarkPolicies.limitingLag(0), 
        						WatermarkEmissionPolicy.emitByFrame(slidingWindowPolicy), 
        						0)
        		)).localParallelism(1);
        dag.edge(Edge.from(averageOf50,1).to(watermarkedAverageOf50));

        // Inject timestamps into the 200-point stream so can group on sliding windows
        Vertex watermarkedAverageOf200 = dag.newVertex("Watermark 200 point Average",
        		Processors.insertWatermarksP(
        				WatermarkGenerationParams.wmGenParams(
        						TimePrice::getTimestamp, 
        						WatermarkPolicies.limitingLag(0), 
        						WatermarkEmissionPolicy.emitByFrame(slidingWindowPolicy), 
        						0)
        		)).localParallelism(1);
        dag.edge(Edge.from(averageOf200,1).to(watermarkedAverageOf200));

        // 50 point stream type, 200 point stream type, grouping class and output type
        AggregateOperation2<TimePrice, TimePrice, TimePriceGrouper, 
        		Tuple2<Date, List<BigDecimal>>> 
        myAggregation = AggregateOperation
        	     .withCreate(TimePriceGrouper::new)
        	     .andAccumulate0(TimePriceGrouper::add0)
        	     .andAccumulate1(TimePriceGrouper::add1)
        	     .andCombine(TimePriceGrouper::combine)
        	     .andExportFinish(TimePriceGrouper::get)
        	     ;

        // Produce a compound of two each from 50-point and 200-point average
        Vertex coGrouper = dag.newVertex("Group 50 and 200 point window",
        		Processors.aggregateToSlidingWindowP(
        				Arrays.asList(
        						DistributedFunctions.constantKey(),
        						DistributedFunctions.constantKey()),
        				Arrays.asList(
        						(DistributedToLongFunction<TimePrice>) TimePrice::getTimestamp,
        						(DistributedToLongFunction<TimePrice>) TimePrice::getTimestamp),
        				TimestampKind.EVENT, 
        				slidingWindowPolicy, 
        				myAggregation,
        				MovingAverageDAG::timestampedEntryBuilder
        				)
        		).localParallelism(1);
        dag.edge(Edge.from(watermarkedAverageOf50,0).to(coGrouper,0));
        dag.edge(Edge.from(watermarkedAverageOf200,0).to(coGrouper,1));

        // Filter incomplete output from coGrouper, partial windows
        Vertex filterPartialCoGroup = dag.newVertex("Filter CoGroup",
        		Processors.filterP(
        				(TimestampedEntry<?, Tuple2<Date, List<BigDecimal>>> entry) -> 
        					entry.getValue() != null)
        			);
        dag.edge(Edge.from(coGrouper).to(filterPartialCoGroup));

        // Look for Death Cross and Golden Cross
        Vertex crossDetector = dag.newVertex("Cross Detector", CrossDetectorProcessor::new);
        dag.edge(Edge.from(filterPartialCoGroup).to(crossDetector).isolated());

        // Sink detected cross as formatted text to a topic for subscribers
        Vertex alertTopicSink = dag.newVertex("Alert Topic Sink", AlertToTopicProcessor::new);
        dag.edge(Edge.between(crossDetector,alertTopicSink).isolated());
        
        // Sink detected cross as source info to a map for future reference
        Vertex alertMapSink = dag.newVertex("Alert IMap Sink",
        		SinkProcessors.writeMapP(MyConstants.IMAP_NAME_ALERT)
        		);
        dag.edge(Edge.from(crossDetector,1).to(alertMapSink));
        
        return dag;
    }

	/**
	 * <p>For routing on Currency pairs, whole key.
	 * </p>
	 * 
	 * @return
	 */
    public static DistributedFunction<ConsumerRecord<CurrencyPairKey, CurrencyPairValue>, CurrencyPairKey> currencyPairKey() {
        return ConsumerRecord::key;
    }
    
    /**
     * <p>Build a timestamped entry, like a map entry but with a
     * timestamp too :-)
     * </p>
     */
    public static TimestampedEntry<String, Tuple2<Date,List<BigDecimal>>> timestampedEntryBuilder(
    		long previous, long current, String key, Tuple2<Date,List<BigDecimal>> value
    		) {
    	return new TimestampedEntry<>(current, key, value);
    }
}
