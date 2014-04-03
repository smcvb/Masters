package aggregators;
import org.apache.hadoop.io.FloatWritable;

/**
 * Averages the result of the {@link FloatAbsDiffAggregator}.
 */
public class FloatAverageAggregator extends FloatAbsDiffAggregator {
	@Override
	public FloatWritable finalizeAggregation() {
		return new FloatWritable(getValue().get() / getTimesAggregated().get());
	}
}
