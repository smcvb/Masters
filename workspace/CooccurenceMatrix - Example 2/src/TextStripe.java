import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Wrapper for MapWritable to form a stripe
 * 
 * @author stevenb
 * @date 26-03-2013
 */
public class TextStripe extends MapWritable {
	
	public TextStripe(){
		super();
	}
	
	public void addStripe(TextStripe stripe){
		for(Entry<Writable, Writable> e : stripe.entrySet()){
			if(this.containsKey((Text)e.getKey())){
				this.increment((Text)e.getKey(), (IntWritable)e.getValue());
			} else {
				this.put((Text)e.getKey(), (IntWritable)e.getValue());
			}
		}
	}
	
	public void increment(Text word){
		increment(word, (IntWritable)this.get(word));
	}
	
	public void increment(Text word, IntWritable count){
		count.set(count.get() + 1);
		this.put(word, count);
	}
	
	public String toString(){
		String output = "";
		for(Entry<Writable, Writable> e : this.entrySet()){
			output = output + "| |" + e.getKey().toString() + " - " + e.getValue().toString();
		}
		return output;
	}
}