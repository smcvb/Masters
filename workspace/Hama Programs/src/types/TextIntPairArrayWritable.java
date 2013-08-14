package types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Extension of the ArrayWritable to store 
 *  Text/IntWritable pairs
 * @author stevenb
 * @date 01-08-2013
 */
public class TextIntPairArrayWritable extends ArrayWritable {
	
	public TextIntPairArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}
	
	@Override
	public String toString() {
		String[] strings = toStrings();
		StringBuilder b = new StringBuilder();
		for (int i = 0; i < strings.length; i++) {
			b.append(strings[i]);
		}
		return b.toString();
	}
}
