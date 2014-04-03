/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package cloud9;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import cloud9.IndexableFileInputFormat;
import cloud9.XMLInputFormat;
import cloud9.XMLInputFormat.XMLRecordReader;
import cloud9.language.WikipediaPageFactory;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia pages from the XML dumps.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 * @author (alteration) Steven van Beelen
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {
	
	/**
	 * Returns a {@code RecordReader} for this {@code InputFormat}.
	*/
	public RecordReader<LongWritable, WikipediaPage> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		return new WikipediaPageRecordReader();
	}

	/**
	 * Hadoop {@code RecordReader} for reading Wikipedia pages from the XML dumps.
	 */
	public static class WikipediaPageRecordReader extends RecordReader<LongWritable, WikipediaPage> {
		
		private String language;
		private final XMLRecordReader reader = new XMLRecordReader();
		private WikipediaPage wikipage;
		
		/**
		 * Creates a {@code WikipediaPageRecordReader}.
		 * @throws InterruptedException 
		 */
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);
			language = conf.get("wiki.language");
			wikipage = WikipediaPageFactory.createWikipediaPage(language);
			reader.initialize(inputSplit, context);
		}

		/**
		 * Reads the next key-value pair.
		 */
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}
		
		/**
		 * Creates an object for the key.
		 */
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		/**
		 * Creates an object for the value.
		 */
		public WikipediaPage getCurrentValue() throws IOException, InterruptedException {
			WikipediaPage.readPage(wikipage, reader.getCurrentValue().toString());
			return wikipage;
		}
		
		/**
		 * Returns progress on how much input has been consumed.
		 */
		public float getProgress() throws IOException {
			return reader.getProgress();
		}

		/**
		 * Closes this InputSplit.
		 */
		public void close() throws IOException {
			wikipage = null;
			reader.close();
		}
	}
}