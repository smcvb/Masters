package types;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	
	private Text term,
				docid;
	
	public TextPair() {
		this.term = new Text();
		this.docid = new Text();
	}
	
	public TextPair(Text term, Text docid) {
		this.term = term;
		this.docid = docid;
	}
	
	public TextPair(String term, String docid) {
		this(new Text(term), new Text(docid));
	}
	
	public void set(TextPair pair){
		this.term = pair.getTerm();
		this.docid = pair.getDocid();
	}
	
	public void set(Text term, Text docid) {
		this.term = term;
		this.docid = docid;
	}
	
	public void set(String term, String docid) {
		this.term.set(term);
		this.docid.set(docid);
	}
	
	//get-set term Text variable
	public Text getTerm() { return term; }
	public void setTerm(String term){ this.term = new Text(term); }
	
	//get-set docid LongWritable variable
	public Text getDocid() { return docid; }
	public void setDocid(String docid){ this.docid = new Text(docid); }
	
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
		if(this == o) 
			return true;
		if(o == null || getClass() != o.getClass())
			return false;
		TextPair tp = (TextPair)o;
		if(term != null ? !term.equals(tp.getTerm()) : tp.getTerm() != null)
			return false;
		if(docid != null ? !docid.equals(tp.getDocid()) : tp.getDocid() != null)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return term + "\t" + docid + " ";
	}

	@Override
	public int compareTo(TextPair tp) {
		int cmp = this.term.compareTo(tp.getTerm());
		if (cmp != 0) {
			return cmp;
		}
		return this.docid.compareTo(tp.getDocid());
	}
}