import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic Text-IntWritable Pair class implementation for Hama/Hadoop
 * 
 * @date 11-04-2013
 */
public class TextIntWritablePair implements WritableComparable<TextIntWritablePair> {
	
	private Text term;
	private IntWritable count;
	
	public TextIntWritablePair() {
		this.term = new Text();
		this.count = new IntWritable();
	}
	
	public TextIntWritablePair(Text term, IntWritable count) {
		this.term = term;
		this.count = count;
	}
	
	public TextIntWritablePair(String term, int count) {
		this(new Text(term), new IntWritable(count));
	}
	
	public void set(TextIntWritablePair pair){
		this.term = pair.getTerm();
		this.count = pair.getCount();
	}
	
	public void set(Text term, IntWritable count) {
		this.term = term;
		this.count = count;
	}
	
	public void set(String term, int count) {
		this.term.set(term);
		this.count.set(count);
	}
	
	//get-set term Text variable
	public Text getTerm() { return term; }
	public void setTerm(String term){ this.term = new Text(term); }
	
	//get-set count Text variable
	public IntWritable getCount() { return count; }
	public void setCount(int count){ this.count = new IntWritable(count); }
	
	@Override
	public void write(DataOutput out) throws IOException {
		term.write(out);
		count.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		term.readFields(in);
		count.readFields(in);
	}
	
	@Override
	public int hashCode() {
		int result = term != null ? term.hashCode() : 0;
		result = 163 * result + (count != null ? count.hashCode() : 0);
		return result;
	}
	
	@Override
	public boolean equals(Object o){ 
		if(this == o) 
			return true;
		if(o == null || getClass() != o.getClass())
			return false;
		TextIntWritablePair tp = (TextIntWritablePair)o;
		if(count != null ? !count.equals(tp.getCount()) : tp.getCount() != null)
			return false;
		if(term != null ? !term.equals(tp.getTerm()) : tp.getTerm() != null)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return term + "\t" + count;
	}

	@Override
	public int compareTo(TextIntWritablePair tp) {
		int cmp = this.term.compareTo(tp.term);
		if (cmp != 0) {
			return cmp;
		}
		return this.count.compareTo(tp.count);
	}
}