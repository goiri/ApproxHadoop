package org.apache.hadoop.mapreduce.approx.multistage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * A partitioner that checks if we have a parameter and send it to the specified one. \0PARAMETER-1-1 -> 1
 */
public class ParameterPartitioner<K,V> extends HashPartitioner<K,V> {
	/**
	 * Overwrite the partitioner to send to everybody.
	 */
	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		if (key instanceof Text) {
			String aux = ((Text)key).toString();
			
			// Check if it's a parameter, it has the \0 first to guarantee is the first when sorting
			int lastIndex = aux.lastIndexOf('-');
			if (aux.charAt(0) == '\0' && lastIndex>0) {
				// We send it to the reducer specified in the parameter
				return Integer.parseInt(aux.substring(lastIndex+1)) % numReduceTasks;
			}
		}
		return super.getPartition(key, value, numReduceTasks);
	}
}
