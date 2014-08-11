package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingMapper;

/**
 * Mapper that checks the size of wikipedia articles.
 */
public class WikiLengthsMapper extends MultistageSamplingMapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);
	
	public static enum mapCounters { NUMPAGES, MAPID }
	
	/**
	 * Map to link size to numer of pages.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Size -> One page [We group in 100s]
		//context.write(new LongWritable((value.getLength()/100)*100), one);
		//context.write(new Text(Integer.toString((value.getContent().length()/100)*100)), one);
		context.write(new Text(Integer.toString((value.getLength()/100)*100)), one);
		
		// Offset -> Page size
		//context.write(key, new LongWritable(value.getLength()));
		
		// Count the number of pages
		context.getCounter(mapCounters.NUMPAGES).increment(1);
	}
}
