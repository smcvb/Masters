package types;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class TextIntPairArrayWritable extends ArrayWritable {

	public TextIntPairArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	public String toString(){
		String[] strings = this.toStrings();
		String s = "";
		for(int i = 0; i < strings.length; i++)
			s = s + strings[i];
		return s;
	}
}
