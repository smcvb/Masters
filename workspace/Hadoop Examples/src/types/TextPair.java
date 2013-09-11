package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic Text-Pair class implementation for Hadoop
 * 
 * @from Hadoop: The Definitive Guide
 * @date 25-03-2013
 */
public class TextPair implements WritableComparable<TextPair> {
	
	private Text first;
	private Text second;
	
	public TextPair() {
		set(new Text(), new Text());
	}
	
	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}
	
	public TextPair(Text first, Text second) {
		set(first, second);
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public void set(String first, String second) {
		this.first = new Text(first);
		this.second = new Text(second);
	}
	
	//get-set first Text variable
	public Text getFirst() {
		return first;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	
	public void setFirst(String first) {
		this.first = new Text(first);
	}
	
	//get-set second Text variable
	public Text getSecond() {
		return second;
	}
	
	public void setSecond(Text second) {
		this.second = second;
	}
	
	public void setSecond(String second) {
		this.second = new Text(second);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return first + "\t" + second;
	}
	
	@Override
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}
