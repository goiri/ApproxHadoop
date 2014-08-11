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

package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.SamplingRecordReader;

import org.apache.hadoop.mapreduce.wikipedia.IndexableFileInputFormat;
import org.apache.hadoop.mapreduce.approx.XMLInputFormat;
import org.apache.hadoop.mapreduce.approx.XMLInputFormat.XMLRecordReader;
import org.apache.hadoop.mapreduce.wikipedia.language.WikipediaPageFactory;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia pages from the XML dumps.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {
	@Override
	public RecordReader<LongWritable, WikipediaPage> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new WikipediaPageRecordReader();
	}

	/**
	 * Uses the XMLRecordReader to read wikipedia articles.
	 */
	public static class WikipediaPageRecordReader extends RecordReader<LongWritable, WikipediaPage> implements SamplingRecordReader {
		private XMLRecordReader reader = new XMLRecordReader();
		private WikipediaPage page;
		private String language;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY,   WikipediaPage.XML_END_TAG);
			
			language = conf.get("wiki.language", "en"); // Assume 'en' by default.
			page = WikipediaPageFactory.createWikipediaPage(language);

			reader.initialize(split, context);
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public WikipediaPage getCurrentValue() throws IOException, InterruptedException {
			WikipediaPage.readPage(page, reader.getCurrentValue().toString());
			return page;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}
		
		/**
		 * Return the sampling ratio (M/m).
		 */
		@Override
		public int getSamplingRatio() {
			return reader.getSamplingRatio();
		}
		
		/**
		 * Return the population (M).
		 */
		@Override
		public long getPopulation() {
			return reader.getPopulation();
		}
	}
}
