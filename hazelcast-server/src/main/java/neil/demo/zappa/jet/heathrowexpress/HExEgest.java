package neil.demo.zappa.jet.heathrowexpress;

import java.util.Collections;
import java.util.Map;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.pipeline.JournalInitialPosition;

import neil.demo.zappa.Gps;
import neil.demo.zappa.MyConstants;

/**
 * <p>Analyse Hazelcast's history of recent positions
 * for the Heathrow Express and dump them to a map.
 * </p>
 */
public class HExEgest {
    private static final int TEN_SECONDS_IN_MS = 10 * 1000;
    private static final int ONE_MINUTE_IN_MS = 60 * 1000;

	public static DAG build() {
	
        /* Create a processing graph
         */
        DAG dag = new DAG();

        /* Define a time based data window - 1 minute long and advancing to a new window every
         * ten second. Therefore overlapping, some data in consecutive windows.
         */
        SlidingWindowPolicy windowDefinition = 
                        SlidingWindowPolicy.slidingWinPolicy(ONE_MINUTE_IN_MS, TEN_SECONDS_IN_MS);

        /* Define an aggregator on a single stream, type <Input, Aggregator, Output>, that takes a series of
         * positions and "aggregates" these together reducing them to a speed.
         */
        AggregateOperation1<TimestampedEntry<String,Gps>, GpsAggregation, Map.Entry<String, Double>> gpsAggregation 
                        = AggregateOperation
        .withCreate(GpsAggregation::new)
        .andAccumulate(GpsAggregation::accumulate)
        .andCombine(GpsAggregation::combine)
        .andExportFinish(GpsAggregation::finish);

        /* Watermark the input with timestamps
         */
        @SuppressWarnings("rawtypes")
        WatermarkGenerationParams watermarkGenerationParams
                = WatermarkGenerationParams.wmGenParams(
                                HExEgest.extractTimestamp(), 
                                WatermarkPolicies.limitingLag(0), 
                                WatermarkEmissionPolicy.emitByFrame(windowDefinition), 
                                0);

        /* Define the steps of processing, see diagram above
         */
        @SuppressWarnings("unchecked")
        Vertex step1 = dag.newVertex("eventJournal", 
                        SourceProcessors.streamMapP(
                                        MyConstants.IMAP_NAME_POSITION, 
                                        JournalInitialPosition.START_FROM_OLDEST,
                                        watermarkGenerationParams
                                        )
                        );

        Vertex step2 = dag.newVertex("projection",
        				Processors.mapP((Map.Entry<String,Gps> entry) 
                        -> new TimestampedEntry<>(entry.getValue().getTimestamp(), entry.getKey(), entry.getValue()
                        		)));

        @SuppressWarnings("unchecked")
        Vertex step3 = dag.newVertex("punctuation",
                        Processors.insertWatermarksP(watermarkGenerationParams)
                        );

        Vertex step4 = dag.newVertex("aggregate",
                Processors.aggregateToSlidingWindowP(
                                Collections.singletonList((DistributedFunction<TimestampedEntry<?, ?>, ?>) TimestampedEntry::getKey),
                                Collections.singletonList((DistributedToLongFunction<TimestampedEntry<?, ?>>) TimestampedEntry::getTimestamp),
                                TimestampKind.EVENT, 
                                windowDefinition,
                                gpsAggregation,
                                TimestampedEntry::fromWindowResult
                                ));

        Vertex step5 = dag.newVertex("projection2",
        				Processors.mapP((TimestampedEntry<String,Map.Entry<String,Double>> entry) 
        						-> entry.getValue()
        						));
        
        // Insufficient data for speed to be calculated
        Vertex step6 = dag.newVertex("filter1",
        		Processors.filterP((Map.Entry<String,Double> entry) -> !entry.getValue().isNaN())
						);

        Vertex step7 = dag.newVertex("filter2",
        		Processors.filterP((Map.Entry<String,Double> entry) -> (entry.getValue() > 0))
						);

		Vertex step8 = dag.newVertex("logger", MyPassThroughLogger::new);
		
		Vertex step9 = dag.newVertex("mapSink", SinkProcessors.writeMapP(MyConstants.IMAP_NAME_SPEED));

		/* Plumbing, this is an easy chain!
         */
		dag.edge(Edge.between(step1, step2));
		dag.edge(Edge.between(step2, step3));
        dag.edge(Edge.between(step3, step4));
        dag.edge(Edge.between(step4, step5));
        dag.edge(Edge.between(step5, step6));
        dag.edge(Edge.between(step6, step7));
        dag.edge(Edge.between(step7, step8));
        dag.edge(Edge.between(step8, step9));

        return dag;
	}

	
	/**
	 * <p>Helper, pull the timestamp as a long out of the GPS point
	 * </p>
	 */
	public static DistributedToLongFunction<Map.Entry<String,Gps>> 
        extractTimestamp() {
			return entry -> entry.getValue().getTimestamp();
	}
}
