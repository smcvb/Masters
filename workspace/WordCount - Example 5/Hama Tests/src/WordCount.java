import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

/**
 * WordCount example
 * Implementation in Apache Hama
 * 
 * To test out how to use BSP framework
 *
 * @author stevenb
 * @version one
 * @date 15-04-2013
 */
public class WordCount {
	
	public static final int TASKS = 5;
	
	public static class WordCountBSP extends BSP<LongWritable, Text, Text, IntWritable, TextIntWritablePair>{

		private HashMap<String, Integer> wordMap;
		
		public void setup(BSPPeer<LongWritable, Text, Text, IntWritable, TextIntWritablePair> peer) throws IOException { //initialize
			LongWritable key = new LongWritable(); //WordCount map phase
			Text value = new Text();
			wordMap = new HashMap<String, Integer>();
			while(peer.readNext(key, value)){
				String termString = "",
						line = value.toString();
				String[] terms = line.split("\\s");
				for(int i = 0; i < terms.length; i++){
					termString = terms[i].toLowerCase();
					wordMap.put(termString, (wordMap.containsKey(termString) ? wordMap.get(termString) + 1 : 1)); 
				}
			}
		}
		
		public void bsp(BSPPeer<LongWritable, Text, Text, IntWritable, TextIntWritablePair> peer) throws IOException, InterruptedException, SyncException {
			for(Entry<String, Integer> entry : wordMap.entrySet()){ //WordCount shuffle-sort phase
				Text term = new Text(entry.getKey());
				IntWritable count = new IntWritable(entry.getValue());
				TextIntWritablePair pair = new TextIntWritablePair(term, count);
				String other = peer.getPeerName(Math.abs(term.hashCode()) % peer.getNumPeers());
				peer.send(other, pair);
			}
			peer.sync();
			wordMap.clear();
			int totalMessages = peer.getNumCurrentMessages();
			for(int i = 0; i < totalMessages; i++){ //WordCount reduce phase
				TextIntWritablePair pair = peer.getCurrentMessage();
				String termString = pair.getTerm().toString();
				int counter = pair.getCount().get();
				wordMap.put(termString, (wordMap.containsKey(termString) ? wordMap.get(termString) + counter : counter));
			}
		}
		
		public void cleanup(BSPPeer<LongWritable, Text, Text, IntWritable, TextIntWritablePair> peer) throws IOException { //close
			for(Entry<String, Integer> entry : wordMap.entrySet()){
				Text term = new Text(entry.getKey());
				IntWritable count = new IntWritable(entry.getValue());
				peer.write(term, count);
			}
		}
	}
	
	public static BSPJob createJob(String[] args) throws IOException {
		BSPJob job = new BSPJob();
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCount Example");
		
		if(args.length < 2) {
			System.out.println("usage:\t <input path> <output path> <number of tasks [default = 5]>");
			System.exit(-1);
		}
		
		job.setBspClass(WordCountBSP.class);
		job.setInputPath(new Path(args[0]));
		job.setOutputPath(new Path(args[1]));
		job.setInputFormat(TextInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if(args.length == 3) {
			job.setNumBspTask(Integer.parseInt(args[2]));
		} else 
			job.setNumBspTask(TASKS);
		
		return job;
	}
	
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		BSPJob job = createJob(args); 
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}