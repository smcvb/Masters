import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.queue.SortedMessageQueue;
import org.apache.hama.bsp.sync.SyncException;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextTextIntWritableTuple;

public class InvertedIndex {
	
	public static final int TASKS = 5;
	
	public static class InvertedIndexBSP extends BSP<LongWritable, Text, Text, TextIntPairArrayWritable, TextTextIntWritableTuple>{

		private String currentTerm, previousTerm;
		private HashMap<String, Integer> postingTupleMap;
		private HashMap<String, TextIntPair> completeTupleMap;
		private ArrayList<TextIntPair> postingsList;
		private TextIntPairArrayWritable writablePostings;
		
		public void setup(BSPPeer<LongWritable, Text, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException { //initialize
			currentTerm = "";
			previousTerm = null;
			postingTupleMap = new HashMap<String, Integer>();
			completeTupleMap = new HashMap<String, TextIntPair>();
			postingsList = new ArrayList<TextIntPair>();
			writablePostings = new TextIntPairArrayWritable(TextIntPair.class);
		}
		
		public void bsp(BSPPeer<LongWritable, Text, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException, InterruptedException, SyncException {
			int totalMessages = 0;
			String filename = Constants.JOB_INPUT_DIR;
			LongWritable key = new LongWritable();
			Text value = new Text();
			
			peer.getConfiguration().writeXml(System.out);
			
			while(peer.readNext(key, value)){
				String term = "",
						line = value.toString();
				String[] terms = line.split("\\s+");
				for(int i = 0; i < terms.length; i++){ //Payload part of the inverted indexing algorithm | term frequency in this case
					term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
					if(!term.equals(""))
						postingTupleMap.put(term, (postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1)); 
				}
			}
			for(Entry<String, Integer> entry : postingTupleMap.entrySet()){
				TextTextIntWritableTuple tuple = new TextTextIntWritableTuple(entry.getKey(), filename, entry.getValue());
				String other = peer.getPeerName(Math.abs(tuple.getTerm().hashCode()) % peer.getNumPeers());
				peer.send(other, tuple);
			}
			
			postingTupleMap.clear();//empty memory
			peer.sync();
			totalMessages = peer.getNumCurrentMessages();
			for(int i = 0; i < totalMessages; i++){ //WordCount reduce phase
				TextTextIntWritableTuple tuple = peer.getCurrentMessage();
				currentTerm = tuple.getTerm().toString();
				if(!currentTerm.equals(previousTerm) && previousTerm != null){ //new term start, write out old term
					if(!completeTupleMap.containsKey(currentTerm)){
						TextIntPair posting = new TextIntPair(tuple.getDocid(), tuple.getFrequency());
						completeTupleMap.put(currentTerm, posting);
					} else {
						TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
						postingsArray = postingsList.toArray(postingsArray);
						writablePostings.set(postingsArray);
						peer.write(new Text(previousTerm), writablePostings);
						postingsList.clear();//empty memory
					}
				}
				Text postingText = new Text(tuple.getDocid().toString());
				IntWritable postingIntWritable = new IntWritable(tuple.getFrequency().get());
				TextIntPair posting = new TextIntPair(postingText, postingIntWritable);
				postingsList.add(posting);
				previousTerm = currentTerm;
			}
		}
		
		public void cleanup(BSPPeer<LongWritable, Text, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException { //close
			TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
			postingsArray = postingsList.toArray(postingsArray);
			writablePostings.set(postingsArray);
			peer.write(new Text(previousTerm), writablePostings);
			postingsList.clear();//empty memory
		}
	}
	
	public static void printUsage(int argLength){
		if(argLength < 2) {
			System.out.println("usage:\t <input path> <output path> <number of tasks [default 5]>");
			System.exit(-1);
		}
	}
	
	public static BSPJob createJob(HamaConfiguration conf, String[] args) throws IOException{
		printUsage(args.length);
		conf.set(MessageManager.QUEUE_TYPE_CLASS, "org.apache.hama.bsp.message.queue.SortedMessageQueue");
		
		BSPJob job = new BSPJob(conf, InvertedIndex.class);
		job.setJobName("Inverted Indexing");
		job.setBspClass(InvertedIndexBSP.class);
		job.setInputPath(new Path(args[0]));
		job.setOutputPath(new Path(args[1]));
		job.setInputFormat(TextInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if(args.length > 2)
			job.setNumBspTask(Integer.parseInt(args[2]));
		if(args.length == 2)
			job.setNumBspTask(TASKS);
		
		System.out.printf("WorkingDirectory: %s\n", job.getWorkingDirectory().getName().toString());
		
		return job;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		BSPJob job = createJob(conf, args);
		
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true))
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}