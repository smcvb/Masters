import org.apache.hadoop.io.FloatWritable;
import org.apache.hama.graph.AbstractAggregator;
import org.apache.hama.graph.Vertex;

public class FloatAbsDiffAggregator extends AbstractAggregator<FloatWritable, Vertex<?, ?, FloatWritable>> {
	
	float absoluteDifference = 0.0f;
	float absoluteDifferenceTwo = 0.0f;
	
	@Override
	public void aggregate(Vertex<?, ?, FloatWritable> v, FloatWritable oldValue, FloatWritable newValue) {
		// make sure it's nullsafe
		if (oldValue != null) {
			absoluteDifferenceTwo += Math.abs(oldValue.get() - newValue.get());
			System.out.printf("AGGREGATEOLDNEW\tabsoluteDifference %f absoluteDifferenceTwo %f\n", absoluteDifference, absoluteDifferenceTwo);
		}
	}
	
	// when a master aggregates he aggregated values, he calls this, so let's just
	// sum up here.
	@Override
	public void aggregate(Vertex<?, ?, FloatWritable> vertex, FloatWritable value) {
		absoluteDifference += value.get();
		System.out.printf("AGGREGATE\tabsoluteDifference %f absoluteDifferenceTwo %f\n", absoluteDifference, absoluteDifferenceTwo);
	}

	@Override
	public FloatWritable getValue() {
		System.out.printf("GET\tabsoluteDifference %f absoluteDifferenceTwo %f\n", absoluteDifference, absoluteDifferenceTwo);
		return new FloatWritable(absoluteDifference);
	}
}
