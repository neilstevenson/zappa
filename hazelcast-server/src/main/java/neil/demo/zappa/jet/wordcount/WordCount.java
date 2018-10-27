package neil.demo.zappa.jet.wordcount;

import java.util.regex.Pattern;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperations;

import com.hazelcast.jet.function.DistributedFunctions;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import neil.demo.zappa.MyConstants;
 

/**
 * <p>The classic "<i>Word Count</i>". Spot the mistake ?!
 * </p>
 */
public class WordCount {
	private static final Pattern WORDS_PATTERN = Pattern.compile("\\W+");
	
	public static Pipeline build() {
		Pipeline pipeline = Pipeline.create();
		
		pipeline.drawFrom(Sources.<Integer, String>map(MyConstants.IMAP_NAME_HAMLET))
				.flatMap(entry -> Traversers.traverseArray(WORDS_PATTERN.split(entry.getValue())))
				.map(String::toLowerCase)
				.filter(s -> s.length() > 2)
				.groupingKey(DistributedFunctions.wholeItem())
				.aggregate(AggregateOperations.counting())
				.drainTo(Sinks.map(MyConstants.IMAP_NAME_WORDS));

		return pipeline;
	}

}
