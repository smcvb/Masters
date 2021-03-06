package types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Extension of the ArrayWritable to store longs
 * @author stevenb
 * @date 01-08-2013
 */
public class LongArrayWritable extends ArrayWritable {
	
	public LongArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}
	
	@Override
	public String toString() {
		String[] strings = toStrings();
		StringBuilder b = new StringBuilder();
		for (String string : strings) {
			b.append(string);
			b.append("\t");
		}
		return b.toString();
	}
}
