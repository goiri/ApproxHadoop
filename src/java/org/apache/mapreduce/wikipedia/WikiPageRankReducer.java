package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducer;

/**
 * Reducer class. It takes into account the approximation factor.
 */
public class WikiPageRankReducer extends MultistageSamplingReducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	
	/**
	 * Reduce function that uses the default collection approach.
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
