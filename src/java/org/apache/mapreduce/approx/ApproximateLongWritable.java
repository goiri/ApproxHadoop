package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.io.LongWritable;

/**
 * This is just for outputting. The ideal would be to have value as protected in IntWritable but...
 */
public class ApproximateLongWritable extends LongWritable {
	private long value;
	private long range;

	public ApproximateLongWritable(long value, double range) {
		this.value = value;
		this.range = (long) Math.ceil(range);
	}
	
	public String toString() {
		return Long.toString(value)+"+/-"+range;
	}

	private long getValue() {
		return this.value;
	}
	
	private long getRange() {
		return this.range;
	}
	
	/**
	 * Normalized error.
	 */
	public double getError() {
		if (this.value == 0) {
			return 0.0;
		}
		return this.range/this.value;
	}
}