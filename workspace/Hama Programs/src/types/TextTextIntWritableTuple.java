package types;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTextIntWritableTuple implements WritableComparable<TextTextIntWritableTuple> {
	
	private Text term, docid;
	private IntWritable frequency;
	
	public TextTextIntWritableTuple() {
		this.term = new Text();
		this.docid = new Text();
		this.frequency = new IntWritable();
	}
	
	public TextTextIntWritableTuple(Text term, Text docid, IntWritable frequency) {
		this.term = term;
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public TextTextIntWritableTuple(String term, String docid, int frequency) {
		this(new Text(term), new Text(docid), new IntWritable(frequency));
	}
	
	public void set(TextTextIntWritableTuple tuple){
		this.term = tuple.getTerm();
		this.docid = tuple.getDocid();
		this.frequency = tuple.getFrequency();
	}
	
	public void set(Text term, Text docid, IntWritable frequency) {
		this.term = term;
		this.docid = docid;
		this.frequency = frequency;
	}
	
	public void set(String term, String docid, int frequency) {
		this.term.set(term);
		this.docid.set(docid);
		this.frequency.set(frequency);
	}
	
	//get-set term Text variable
	public Text getTerm() { return term; }
	public void setTerm(String term){ this.term = new Text(term); }
	
	//get-set term Text variable
	public Text getDocid() { return docid; }
	public void setDocid(String docid){ this.docid = new Text(docid); }
	
	//get-set frequency IntWritable variable
	public IntWritable getFrequency() { return frequency; }
	public void setfrequency(int frequency){ this.frequency = new IntWritable(frequency); }
	
	@Override
	public void write(DataOutput out) throws IOException {
		term.write(out);
		docid.write(out);
		frequency.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		term.readFields(in);
		docid.readFields(in);
		frequency.readFields(in);
	}
	
	@Override
	public int hashCode() {
		int result = term != null ? term.hashCode() : 0;
		result = 163 * result + (docid != null ? docid.hashCode() : 0);
		result = 163 * result + (frequency != null ? frequency.hashCode() : 0);
		return result;
	}
	
	@Override
	public boolean equals(Object o){ 
		if(this == o) 
			return true;
		if(o == null || getClass() != o.getClass())
			return false;
		TextTextIntWritableTuple tp = (TextTextIntWritableTuple)o;
		if(frequency != null ? !frequency.equals(tp.getFrequency()) : tp.getFrequency() != null)
			return false;
		if(docid != null ? !docid.equals(tp.getDocid()) : tp.getDocid() != null)
			return false;
		if(term != null ? !term.equals(tp.getTerm()) : tp.getTerm() != null)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return term + "\t" + docid + "\t" + frequency;
	}

	@Override
	public int compareTo(TextTextIntWritableTuple tp) {
		int cmp = this.term.compareTo(tp.term);
		if (cmp != 0) {
			return cmp;
		}
		cmp = this.docid.compareTo(tp.docid);
		if (cmp != 0) {
			return cmp;
		}
		return this.frequency.compareTo(tp.frequency);
	}
}