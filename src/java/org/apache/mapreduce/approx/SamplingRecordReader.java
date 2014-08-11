package org.apache.hadoop.mapreduce.approx;

/**
 * Interface for a RecordReader that allows sampling.
 * @author Inigo Goiri
 */
public interface SamplingRecordReader {
	public int getSamplingRatio();
	public long getPopulation();
}