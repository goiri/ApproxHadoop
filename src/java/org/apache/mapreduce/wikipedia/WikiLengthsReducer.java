package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducer;

/**
 * Aggregate the lenghts of the wikipedia pages using multistage sampling.
 */
public class WikiLengthsReducer extends MultistageSamplingReducer<Text, IntWritable, Text, LongWritable> {
	private LongWritable result = new LongWritable();
	
	/**
	 * Precise version that collects everybody and outputs it.
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
