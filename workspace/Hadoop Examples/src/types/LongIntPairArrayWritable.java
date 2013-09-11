package types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class LongIntPairArrayWritable extends ArrayWritable {
	
	public LongIntPairArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}
	
	@Override
	public String toString() {
		String[] strings = toStrings();
		String s = "";
		for (int i = 0; i < strings.length; i++) {
			s = s + strings[i];
		}
		return s;
	}
}
