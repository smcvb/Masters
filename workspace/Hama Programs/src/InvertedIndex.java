import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.SyncException;

import types.TextIntPair;
import types.TextIntPairArrayWritable;
import types.TextTextIntWritableTuple;
import cloud9.WikipediaPage;
import cloud9.WikipediaPageInputFormat;
import cloud9.language.WikipediaPageFactory;

/**
 * Hama program to run the Inverted Indexing algorithm as
 *  specified in 'Data-Intensive Text Processing with MapReduce'
 * @author stevenb
 * @date 15-07-2013
 */
public class InvertedIndex extends Configured implements Tool {
	
	public static final int BSP_TASKS = 27;
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
	
	public static class InvertedIndexBSP extends BSP<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> {
		
		private String currentTerm, previousTerm;
		private HashMap<String, Integer> postingTupleMap;
		private HashSet<String> stopwordSet;
		private ArrayList<TextIntPair> postingsList;
		private TextIntPairArrayWritable writablePostings;
		
		@Override
		public void setup(BSPPeer<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException { //initialize
			currentTerm = "[empty]";
			previousTerm = null;
			postingTupleMap = new HashMap<String, Integer>();
			stopwordSet = new HashSet<String>();
			for (String stopword : STOPWORDS) {
				stopwordSet.add(stopword);
			}
			postingsList = new ArrayList<TextIntPair>();
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
		public void bsp(BSPPeer<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException, InterruptedException, SyncException {
			createTermFrequencies(peer);
			peer.sync();
			createPostingsList(peer);
		}
		
		/**
		 * Creating the term frequencies per Wikipedia page encountered
		 *  and send to the responsible peer for creating the postings list
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for the reading the read values 
		 * 		and sending messages to other peers
		 */
		private void createTermFrequencies(BSPPeer<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException {
			LongWritable key = new LongWritable();
			WikipediaPage page = WikipediaPageFactory.createWikipediaPage("en");
			
			while (peer.readNext(key, page)) { // Collect frequencies and send postings | One message is One WikipediaPage is One send
				if (page.isEmpty()) { // An empty WikipediaPage was encountered, hence nothing to retrieve terms from
					break;
				}
				
				String term = "", article = page.getTitle(), line = page.getContent();
				String[] terms = line.split("\\s+");
				for (int i = 0; i < terms.length; i++) { // Pay load part | term frequency in this case
					term = terms[i].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
					if (!term.equals("") && !stopwordSet.contains(term)) {
						postingTupleMap.put(term, postingTupleMap.containsKey(term) ? postingTupleMap.get(term) + 1 : 1);
					}
				}
				
				for (Entry<String, Integer> entry : postingTupleMap.entrySet()) {
					TextTextIntWritableTuple tuple = new TextTextIntWritableTuple(entry.getKey(), article, entry.getValue());
					String other = peer.getPeerName(Math.abs(tuple.getTerm().hashCode()) % peer.getNumPeers());
					peer.send(other, tuple);
				}
				postingTupleMap.clear(); // Empty memory
			}
		}
		
		/**
		 * Create a postings list of all the counts per document
		 *  received per term found in all the documents
		 * @param peer: BSPPeer object containing all the information of this peer
		 * @throws IOException for the reading of the received messages 
		 * 		and for writing the output
		 */
		private void createPostingsList(BSPPeer<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException {
			int totalMessages = peer.getNumCurrentMessages();
			for (int i = 0; i < totalMessages; i++) {
				TextTextIntWritableTuple tuple = peer.getCurrentMessage();
				currentTerm = tuple.getTerm().toString();
				if (!currentTerm.equals(previousTerm) && previousTerm != null) { // New term start, write out old term
					TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
					postingsArray = postingsList.toArray(postingsArray);
					writablePostings.set(postingsArray);
					peer.write(new Text(previousTerm), writablePostings);
					postingsList.clear(); // Empty memory
				}
				TextIntPair posting = new TextIntPair(new Text(tuple.getDocid().toString()), new IntWritable(tuple.getFrequency().get()));
				postingsList.add(posting);
				previousTerm = currentTerm;
			}
		}
		
		@Override
		public void cleanup(BSPPeer<LongWritable, WikipediaPage, Text, TextIntPairArrayWritable, TextTextIntWritableTuple> peer) throws IOException { // Close
			TextIntPair[] postingsArray = new TextIntPair[postingsList.size()];
			postingsArray = postingsList.toArray(postingsArray);
			writablePostings.set(postingsArray);
			peer.write(new Text(previousTerm), writablePostings);
			postingsList.clear(); // Empty memory
		}
	}
	
	/**
	 * Create the job.
	 * @param args: String array of arguments
	 * @param conf: a HamaConfiguration Object for the BSP job
	 * @return a finalized BSPJob Object for this BSP job
	 * @throws IOException for creating the BSP job Object
	 */
	public static BSPJob createJob(HamaConfiguration conf, String inputPath, String outputPath, int tasks) throws IOException {
		conf.set("wiki.language", "en");
		conf.set(MessageManager.QUEUE_TYPE_CLASS, "org.apache.hama.bsp.message.queue.SortedMessageQueue"); // Internal Sorting as needed, hence sorted message queue
		
		BSPJob job = new BSPJob(conf, InvertedIndex.class); // Main settings
		job.setJobName("Inverted Indexing");
		job.setBspClass(InvertedIndexBSP.class);
		job.setNumBspTask(tasks);
		job.setInputPath(new Path(inputPath)); // Input settings
		job.setInputFormat(WikipediaPageInputFormat.class);
		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setOutputPath(new Path(outputPath)); // Output settings
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job;
	}
	
	private int printUsage() {
		System.out.println("usage:\t <input path> <output path> <number of tasks [default = 27]>");
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
		int tasks = 0;
		String inputPath = "", outputPath = "";
		HamaConfiguration conf = new HamaConfiguration(getConf());
		
		// Set arguments
		if (args.length < 2) {
			System.err.println("Error: too few parameters given");
			return printUsage();
		}
		inputPath = args[0];
		outputPath = args[1];
		if (args.length >= 3) {
			try {
				tasks = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Error: expected Integer instead of " + args[2]);
				return printUsage();
			}
		} else {
			tasks = BSP_TASKS;
		}
		
		// Create and start a job
		BSPJob job = createJob(conf, inputPath, outputPath, tasks);
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
