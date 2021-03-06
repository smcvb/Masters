package aggregators;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hama.graph.AbstractAggregator;
import org.apache.hama.graph.Vertex;

public class FloatAbsDiffAggregator extends AbstractAggregator<FloatWritable, Vertex<?, ?, FloatWritable>> {
	
	float absoluteDifference = 0.0f;
	float absoluteDifferenceTwo = 0.0f;
	
	@Override
	public void aggregate(Vertex<?, ?, FloatWritable> v, FloatWritable oldValue, FloatWritable newValue) {
		// make sure it's null safe
		if (oldValue != null) {
			absoluteDifferenceTwo += Math.abs(oldValue.get() - newValue.get());
		}
	}
	
	// when a master aggregates he aggregated values, he calls this, so let's just
	// sum up here.
	@Override
	public void aggregate(Vertex<?, ?, FloatWritable> vertex, FloatWritable value) {
		absoluteDifference += value.get();
	}

	@Override
	public FloatWritable getValue() {
		return new FloatWritable(absoluteDifference);
	}
}
