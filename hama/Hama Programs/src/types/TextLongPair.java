package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * An object to store a combination of a
 *  Text and a LongWritable
 * @author stevenb
 * @date 01-08-2013
 */
public class TextLongPair implements WritableComparable<TextLongPair> {
	
	private Text term;
	private LongWritable docid;
	
	public TextLongPair() {
		term = new Text();
		docid = new LongWritable();
	}
	
	public TextLongPair(Text term, LongWritable docid) {
		this.term = term;
		this.docid = docid;
	}
	
	public TextLongPair(String term, long docid) {
		this(new Text(term), new LongWritable(docid));
	}
	
	public void set(TextLongPair pair) {
		term = pair.getTerm();
		docid = pair.getDocid();
	}
	
	public void set(Text term, LongWritable docid) {
		this.term = term;
		this.docid = docid;
	}
	
	public void set(String term, long docid) {
		this.term.set(term);
		this.docid.set(docid);
	}
	
	//get-set term Text variable
	public Text getTerm() {
		return term;
	}
	
	public void setTerm(String term) {
		this.term = new Text(term);
	}
	
	//get-set docid LongWritable variable
	public LongWritable getDocid() {
		return docid;
	}
	
	public void setDocid(long docid) {
		this.docid = new LongWritable(docid);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		term.write(out);
		docid.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		term.readFields(in);
		docid.readFields(in);
	}
	
	@Override
	public int hashCode() {
		int result = term != null ? term.hashCode() : 0;
		result = 163 * result + (docid != null ? docid.hashCode() : 0);
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
		TextLongPair tp = (TextLongPair) o;
		if (term != null ? !term.equals(tp.getTerm()) : tp.getTerm() != null) {
			return false;
		}
		if (docid != null ? !docid.equals(tp.getDocid()) : tp.getDocid() != null) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return term + "\t" + docid + " ";
	}
	
	@Override
	public int compareTo(TextLongPair tp) {
		int cmp = term.compareTo(tp.getTerm());
		if (cmp != 0) {
			return cmp;
		}
		return docid.compareTo(tp.getDocid());
	}
	
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(Text.class);
		}
		
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
		}
	}
	
	static { //register this comparator
		WritableComparator.define(Text.class, new Comparator());
	}
}
