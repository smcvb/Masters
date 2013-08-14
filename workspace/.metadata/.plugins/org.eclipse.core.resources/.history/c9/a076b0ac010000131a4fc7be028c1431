package types;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An object to store a combination of a
 *  Text and a IntWritable
 * @author stevenb
 * @date 01-08-2013
 */
public class TextIntPair implements WritableComparable<TextIntPair> {
	
	private Text docid;
	private IntWritable frequency;
	
	
	public TextIntPair() {
		this.docid = new Text();
		this.frequency = new IntWritable();
	}
	
	public TextIntPair(Text docid, IntWritable frequency) {
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public TextIntPair(String docid, int frequency) {
		this(new Text(docid), new IntWritable(frequency));
	}
	
	public void set(TextIntPair pair){
		this.docid = pair.getDocid();
		this.frequency = pair.getFrequency();
	}
	
	public void set(Text docid, IntWritable frequency) {
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public void set(String docid, int frequency) {
		this.docid.set(docid);
		this.frequency.set(frequency);
	}
	
	//get-set docid LongWritable variable
	public Text getDocid() { return docid; }
	public void setDocid(String docid){ this.docid = new Text(docid); }
	
	//get-set frequency IntWritable variable
	public IntWritable getFrequency() { return frequency; }
	public void setFrequency(int frequency){ this.frequency = new IntWritable(frequency); }
	
	@Override
	public void write(DataOutput out) throws IOException {
		docid.write(out);
		frequency.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		docid.readFields(in);
		frequency.readFields(in);
	}
	
	@Override
	public int hashCode() {
		int result = docid != null ? docid.hashCode() : 0;
		result = 163 * result + (frequency != null ? frequency.hashCode() : 0);
		return result;
	}
	
	@Override
	public boolean equals(Object o) {
		if(this == o) 
			return true;
		if(o == null || getClass() != o.getClass())
			return false;
		TextIntPair tp = (TextIntPair)o;
		if(docid != null ? !docid.equals(tp.getDocid()) : tp.getDocid() != null)
			return false;
		if(frequency != null ? !frequency.equals(tp.getFrequency()) : tp.getFrequency() != null)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return docid + "\t" + frequency + " ";
	}

	@Override
	public int compareTo(TextIntPair tp) {
		int cmp = this.docid.compareTo(tp.getDocid());
		if (cmp != 0) {
			return cmp;
		}
		return this.frequency.compareTo(tp.getFrequency());
	}
}