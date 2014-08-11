package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import java.util.List;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingMapper;

/**
 * Mapper class. It takes a page and parses the links.
 */
public class WikiPageRankMapper extends MultistageSamplingMapper<LongWritable, WikipediaPage, Text, IntWritable> {
	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
	};
	
	IntWritable one = new IntWritable(1);
	
	// The number of pages and links we have processed in this map
	private long numPages = 0;
	// private long numLinks = 0;

	/**
	 * Map function that collects the links for each wikipedia page.
	 */
	public void map(LongWritable key, WikipediaPage p, Context context) throws IOException, InterruptedException {
		context.getCounter(PageTypes.TOTAL).increment(1);
		
		// Check links
		List<String> links = p.extractLinkTargets();
		for (String link : new HashSet<String>(links)) {
			context.write(new Text(link.toLowerCase().replaceAll(" ", "_")), one);
			// To do three-stage sampling we need to know how many links there was in this terciary sample
			//context.write(new Text(link.toLowerCase().replaceAll(" ", "_")), new IntWritable(links.size()));
		}
		
		// Account for pages and links
		numPages++;
// 		numLinks += links.size();
	}
	
	/**
	 * Cleanup function that reports how many pages and links have been processed.
	 * We need to overwrite this to send the terciary sampling.
	 */
	/*public void cleanup(Context context) throws IOException, InterruptedException {
		// We modify "m" and "t" parameters
		setM(numPages);
		setT(numLinks);
		// Make the super class take care of sending the rest
		super.cleanup(context);
	}*/
}
