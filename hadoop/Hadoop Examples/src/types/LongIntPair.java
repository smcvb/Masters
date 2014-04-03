package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Basic LongWritable-IntWritable Pair class implementation for Hadoop
 * 
 * @from Hadoop: The Definitive Guide
 * @date 28-03-2013
 */
public class LongIntPair implements WritableComparable<LongIntPair> {
	
	private LongWritable docid;
	private IntWritable frequency;
	
	public LongIntPair() {
		docid = new LongWritable();
		frequency = new IntWritable();
	}
	
	public LongIntPair(LongWritable docid, IntWritable frequency) {
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public LongIntPair(long docid, int frequency) {
		this(new LongWritable(docid), new IntWritable(frequency));
	}
	
	public void set(LongIntPair pair) {
		docid = pair.getDocid();
		frequency = pair.getFrequency();
	}
	
	public void set(LongWritable docid, IntWritable frequency) {
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public void set(long docid, int frequency) {
		this.docid.set(docid);
		this.frequency.set(frequency);
	}
	
	//get-set docid LongWritable variable
	public LongWritable getDocid() {
		return docid;
	}
	
	public void setDocid(long docid) {
		this.docid = new LongWritable(docid);
	}
	
	//get-set frequency IntWritable variable
	public IntWritable getFrequency() {
		return frequency;
	}
	
	public void setFrequency(int frequency) {
		this.frequency = new IntWritable(frequency);
	}
	
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
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LongIntPair tp = (LongIntPair) o;
		if (docid != null ? !docid.equals(tp.getDocid()) : tp.getDocid() != null) {
			return false;
		}
		if (frequency != null ? !frequency.equals(tp.getFrequency()) : tp.getFrequency() != null) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return docid + "\t" + frequency;
	}
	
	@Override
	public int compareTo(LongIntPair tp) {
		int cmp = docid.compareTo(tp.getDocid());
		if (cmp != 0) {
			return cmp;
		}
		return frequency.compareTo(tp.getFrequency());
	}
}
