import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.SyncException;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextLongPair;
import types.TextLongIntMessage;

/**
 * Hama program to run the Inverted Indexing algorithm as
 *  specified in 'Data-Intensive Text Processing with MapReduce'
 * @author stevenb
 * @date 21-11-2013
 */
public class InvertedIndex extends Configured implements Tool {
	
	public static final int BSP_TASKS = 37; // 26 letters + 10 digits + 1 signs
	public static final int MAX_PAGES_READ = 50000; // The number of pages to parse before performing a superstep
	public static final String[] STOPWORDS = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
			"be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
			"can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
			"each",
			"few", "for", "from", "further",
			"had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
			"i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
			"let's",
			"me", "more", "most", "mustn't", "my", "myself",
			"no", "nor", "not",
			"of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
			"same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
			"than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
			"under", "until", "up",
			"very",
			"was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
			"you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
	}; // A list of stop words which are not important to map
	
	public static class InvertedIndexBSP extends BSP<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> {
		
		private int maxPagesParsed;
		private HashSet<String> stopwordSet;
		private HashMap<String, Integer> postingTupleMap;
		private HashMap<String, ArrayList<TextIntPair>> invertedIndex;
		private TextIntPairArrayWritable writablePostings;
		
		@Override
		public void setup(BSPPeer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> peer) throws IOException { //initialize
			maxPagesParsed = peer.getConfiguration().getInt("maxPagesParsed", 0);
			stopwordSet = new HashSet<String>();
			for (String stopword : STOPWORDS) {
				stopwordSet.add(stopword);
			}
			postingTupleMap = new HashMap<String, Integer>();
			invertedIndex = new HashMap<String, ArrayList<TextIntPair>>();
			writablePostings = new TextIntPairArrayWritable(TextIntPair.class);
		}
		
		/**
		 * The main BSP loop
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for createTermFrequencies(), createPostingsList()
		 *  and the synchronize step
		 * @throws InterruptedException for the synchronize step
		 * @throws SyncException for the synchronize step 
		 */
		@Override
		public void bsp(BSPPeer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> peer) throws IOException, InterruptedException, SyncException {
			boolean finished = false;
			while(!finished){
				finished = createTermFrequencies(peer);
				peer.sync();
				createPostingsList(peer);
			}
		}
		
		/**
		 * Creating the term frequencies per Wikipedia page encountered
		 *  and send to the responsible peer for creating the postings list
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for the reading the read values 
		 * 		and sending messages to other peers
		 */
		private boolean createTermFrequencies(BSPPeer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> peer) throws IOException {
			int pagesParsed = 0;
			TextLongPair key = new TextLongPair();
			IntWritable value = new IntWritable();
			
			while (peer.readNext(key, value)) { // Collect frequencies and send postings | One message is One WikipediaPage is One send
				String term = "", contents = key.getTerm().toString();
				long docid = key.getDocid().get();
				String[] terms = contents.split("\\s+");
				for (int i = 0; i < terms.length; i++) { // Pay load part | term frequency in this case
					term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
					if (!term.equals("") && !stopwordSet.contains(term)) {
						postingTupleMap.put(term, postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1);
					}
				}
				
				for (Entry<String, Integer> entry : postingTupleMap.entrySet()) {
					final TextLongIntMessage tuple = new TextLongIntMessage(entry.getKey(), docid, entry.getValue());
					String other = peer.getPeerName(Math.abs(tuple.getTerm().hashCode()) % peer.getNumPeers());
					peer.send(other, tuple);
				}
				postingTupleMap.clear(); // Empty memory
				
				pagesParsed++;
				if(pagesParsed >= maxPagesParsed){
					return false;
				}
			}
			return true;
		}
		
		/**
		 * Create a postings list of all the counts per document
		 *  received per term found in all the documents
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for the reading of the received messages 
		 * 		and for writing the output
		 */
		private void createPostingsList(BSPPeer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> peer) throws IOException {
			TextLongIntMessage tuple;
			
			while ((tuple = peer.getCurrentMessage()) != null) {
				String term = tuple.getTerm().toString();
				ArrayList<TextIntPair> postingsList = (invertedIndex.containsKey(term) ? invertedIndex.get(term) : new ArrayList<TextIntPair>());
				TextIntPair posting = new TextIntPair(new Text(tuple.getDocid().toString()), new IntWritable(tuple.getFrequency().get()));
				postingsList.add(posting);
				
				invertedIndex.remove(term);
				invertedIndex.put(term, postingsList);
			}
		}
		
		/** 
		 * Write all term, postings list pairs to output
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for write()
		 */
		@Override
		public void cleanup(BSPPeer<TextLongPair, IntWritable, Text, TextIntPairArrayWritable, TextLongIntMessage> peer) throws IOException { // Close
			String term;
			ArrayList<TextIntPair> postingsList;
			
			for(Entry<String, ArrayList<TextIntPair>> entry : invertedIndex.entrySet()) {
				term = entry.getKey();
				postingsList = entry.getValue();
				
				TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
				postingsArray = postingsList.toArray(postingsArray);
				writablePostings.set(postingsArray);
				peer.write(new Text(term), writablePostings);
				postingsList.clear(); // Empty memory
			}
		}
	}
	
	/**
	 * Create the job.
	 * @param args: String array of arguments
	 * @param conf: a HamaConfiguration Object for the BSP job
	 * @return a finalized BSPJob Object for this BSP job
	 * @throws IOException for creating the BSP job Object
	 */
	public static BSPJob createJob(HamaConfiguration conf, String inputPath, String outputPath, int tasks, int maxPagesParsed) throws IOException {
		conf.set("wiki.language", "en");
		conf.setInt("maxPagesParsed", maxPagesParsed);
		conf.set(MessageManager.QUEUE_TYPE_CLASS, "org.apache.hama.bsp.message.queue.SortedMessageQueue"); // Internal Sorting as needed, hence sorted message queue
		
		BSPJob job = new BSPJob(conf, InvertedIndex.class); // Main settings
		job.setJobName("Inverted Indexing Step");
		job.setBspClass(InvertedIndexBSP.class);
		job.setNumBspTask(tasks);
		job.setInputPath(new Path(inputPath)); // Input settings
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setInputKeyClass(TextLongPair.class);
		job.setInputValueClass(IntWritable.class);
		job.setOutputPath(new Path(outputPath)); // Output settings
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextIntPairArrayWritable.class);
		
		return job;
	}
	
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> <number of tasks [default = 37]> <number of files to read before synchronizing [default = 50000]>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	/**
	 * Runs the main program
	 * 
	 * @param args: String array of arguments given at start 
	 * @return -1 in case of error | 0 in case of success
	 * @throws Exception from the createJob() and the waitForCompletion() methods
	 */
	public int run(String[] args) throws Exception {
		int tasks = 0, 
				maxPagesParsed = 0; // The number of pages to parse before performing a superstep
		String inputPath = "", outputPath = "";
		HamaConfiguration conf = new HamaConfiguration(getConf());
		
		// Set arguments
		if (args.length < 2) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputPath = args[0];
		outputPath = args[1];
		if (args.length >= 4) {
			try {
				tasks = Integer.parseInt(args[2]);
				maxPagesParsed = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) {
				System.err.println("Error: expected Integers instead of " + args[2] + " and " + args[3]);
				return printUsage();
			}
		} else if (args.length >= 3) {
			try {
				tasks = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Error: expected Integer instead of " + args[2]);
				return printUsage();
			}
			maxPagesParsed = MAX_PAGES_READ;
		} else {
			tasks = BSP_TASKS;
			maxPagesParsed = MAX_PAGES_READ;
		}
		
		// Create and start a job
		BSPJob job = createJob(conf, inputPath, outputPath, tasks, maxPagesParsed);
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(true)) {
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
		System.exit(result);
	}
}
