import org.apache.hadoop.io.FloatWritable;

/**
 * Averages the result of the {@link FloatAbsDiffAggregator}.
 */
public class FloatAverageAggregator extends FloatAbsDiffAggregator {
	@Override
	public FloatWritable finalizeAggregation() {
		System.out.printf("FINALIZE\tabsoluteDifference %f absoluteDifferenceTwo %f\n", absoluteDifference, absoluteDifferenceTwo);
		return new FloatWritable(getValue().get() / getTimesAggregated().get());
	}
}
