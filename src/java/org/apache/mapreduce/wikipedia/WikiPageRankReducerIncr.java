package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducerIncr;

/**
 * Reducer class. It takes into account the approximation factor.
 */
public class WikiPageRankReducerIncr extends MultistageSamplingReducerIncr<Text,IntWritable,Text,IntWritable> {
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

/*public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	// Calculate aggregated value
	int sum = 0;
	for (IntWritable val : values) {
		sum += val.get();
	}
	
	// Check for the number of pages in the current cluster
	if (key.toString().matches(WikiPageRank.NUMPAGES_SAMPLED_MATCH)) {
		clusterPages[curCluster] = sum;
	// Check for the number of links in the current cluster
	} else if (key.toString().matches(WikiPageRank.NUMLINKS_SAMPLED_MATCH)) {
		clusterLinks[curCluster] = sum;
	// Add to the proper cluster
	} else {
		synchronized(this) {
			// We have to create the cluster structure for new keys
			if (!clusters.containsKey(key.toString())) {
				clusters.put(key.toString(), new int[numCluster]);
			}
			clusters.get(key.toString())[curCluster] = sum;
		}
	}
}*/

/*protected synchronized void checkQuality(Context context, boolean output) throws IOException, InterruptedException {
	double maxError = 0.0;
	double maxErrorRel = 0.0;

	// Get current paramters
	int numMaps = context.getConfiguration().getInt("mapred.map.tasks", -1);
	int skipPages = context.getConfiguration().getInt("mapred.input.approximate.skip", 1);
	
	// Multistage sampling
	int N = numMaps;
	int n = curCluster+1;
	
	// Check if we have done any clustering or everything is together
	if (n==1 && context.getConfiguration().getFloat("mapred.map.approximate.drop.percentage", 1.0f) >= 1.0) {
		System.out.println("There is no clusters");
		N = 1;
	}
	
	// Calculate populations in each cluster (we make it long because the multiplication can cause overflow)
	/*long[] mi = new long[n];
	long[] Mi = new long[n];
	for (int i=0; i<n; i++) {
		// The number of pages sampled in each 2-cluster
		mi[i] = clusterPages[i];
		Mi[i] = mi[i]*skipPages; // This is an approximation based on the sampling ratio
	}* /
	double[] auxmi1 = new double[n]; // Mi/mi
	double[] auxmi2 = new double[n]; // mi/(mi-1)
	for (int i=0; i<n; i++) {
		long mi = clusterPages[i];
		long Mi = mi*skipPages;
		// We use precomputed values
		auxmi1[i] = 1.0*Mi/mi;
		auxmi2[i] = 1.0*Mi*(Mi-mi)/(mi-1.0);
	}
	
	// Get the t-score for the current distribution
	double tscore = 1.96; // By default we use the normal distribution
	try {
		TDistribution tdist = new TDistributionImpl(n-1);
		double confidence = 0.95; // 95% confidence => 0.975
		tscore = tdist.inverseCumulativeProbability(1.0-((1.0-confidence)/2.0)); // 95% confidence 1-alpha
	} catch (Exception e) { }
	
	// Go over all the keys
	Collection<String> keys = clusters.keySet();
	/*if (output) {
		// We sort the keys if we try to output them
		keys = new LinkedList<String>(keys);
		Collections.sort((List<String>)keys);
	}* /
	for (String key : keys) {
		// This comment is the long and understandable version of what we do afterwards
		/*
		// Collect the results for each 2-cluster
		int[] yi  = new int[n];
		int[] yti = new int[n];
		for (int i=0; i<n; i++) {
			// Account on how many pages where linking to this key
			yi[i] = clusters.get(key)[i];
			yti[i] = clusterLinks[i];
		}
		
		// Estimate the results for each 2-cluster
		double[] yhati = new double[n];
		for (int i=0; i<n; i++) {
			yhati[i] = (1.0*Mi[i]/mi[i])*yi[i];
		}
		
		// Estimate the total number
		double tauhat = (1.0*N/n)*sum(yhati);
		
		// Calculate the errors
		// Estimate the global deviation
		double su2 = var(yhati);
		
		// Calculate proportions
		double[] pi = new double[n];
		for (int i=0; i<n; i++) {
			pi[i] = 1.0*yi[i]/yti[i];
		}
		
		// Estimate variance in primary
		double[] si2 = new double[n];
		for (int i=0; i<n; i++) {
			si2[i] = (1.0*mi[i]/(mi[i]-1.0)) * pi[i] * (1.0-pi[i]);
		}
		
		// Calculate total variance
		double var1 = 1.0*(N*(N-n)*su2)/n;
		double var2 = 0.0;
		for (int i=0; i<n; i++) {
			var2 += 1.0*(Mi[i]*(Mi[i]-mi[i])*si2[i])/mi[i];
		}
		var2 = (1.0*N/n)*var2;
		double var3 = 0.0; // var3 stays 0 because ti=Ti (var ~= Ti*(Ti-ti)*st2/ti)
		
		// Variation and standard error for the final result
		double vartauhat = var1 + var2 + var3;
		double setauhat = Math.sqrt(vartauhat);
		* /
		
		// This is the dirty and more efficient version
		double[] yhati = new double[n];
		double var2 = 0.0;
		double tauhat = 0.0;
		for (int i=0; i<n; i++) {
			int yi = clusters.get(key)[i];
			int yti = clusterLinks[i];
			yhati[i] = auxmi1[i]*yi;
			tauhat += (1.0*N/n)*yhati[i];
			double pi = 1.0*yi/yti;
			var2 += auxmi2[i] * pi * (1.0-pi);
		}
		// Standard error for the final result
		double setauhat = Math.sqrt(1.0*(N*(N-n)*var(yhati))/n + (1.0*N/n)*var2);
		
		// DEBUG
		/*if (key.toString().equals("united_states") || key.toString().equals("france")) {
			System.out.format("%d  -> %s = %.2f +/- %.2f (+/-%.2f%%)\n", n, key, tauhat, tscore*setauhat, 100.0*tscore*setauhat/tauhat);
		}* /
		
		// Check which is the maximum error
		if (tscore*setauhat == Double.NaN) {
			maxError = Double.MAX_VALUE;
		} else if (tscore*setauhat > maxError) {
			maxError = tscore*setauhat;
			maxErrorRel = 100.0*tscore*setauhat/tauhat;
		}
		
		// We save the output
		if (output) {
			//context.write(new Text(key), new IntWritable((int) tauhat));
			context.write(new Text(key), new ApproximateIntWritable((int) tauhat, tscore*setauhat));
		}
	}
	
	// Check if we can start dropping
	if (n>1) {
		System.out.format(new Date() + ": Max error with %d maps is +/-%.2f (+/-%.2f)\n", n, maxError, maxErrorRel);
		//if (maxError < 10*1000) {
		if (maxErrorRel < 10.0) { // >10%
			System.out.format("With this error, we can drop!\n");
			context.setStatus("dropping");
		}
	}
}*/
