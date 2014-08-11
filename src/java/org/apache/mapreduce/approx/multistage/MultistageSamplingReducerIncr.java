package org.apache.hadoop.mapreduce.approx.multistage;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import java.util.Iterator;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

import org.apache.hadoop.mapreduce.approx.lib.input.ApproximateLineRecordReader;

import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;
import org.apache.hadoop.mapreduce.approx.ApproximateIntWritable;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.util.Progressable;

import org.apache.log4j.Logger;

/**
 * Perform multistage sampling.
 * This reducer stores all the memory and periodically checks the quality of the results.
 * It can decide to drop dynamically.
 * @author Inigo Goiri
 */
public abstract class MultistageSamplingReducerIncr<KEYIN extends Text,VALUEIN,KEYOUT,VALUEOUT extends WritableComparable> extends MultistageSamplingReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger(MultistageSamplingReducerIncr.class);

	// Detect if we get a new cluster (it would break sorting)
	private Text prevKey;
	
	// Actual clusters (we do everything in doubles to not lose any precission)
	private Map<String,SparseArray<Double>> clusters; // y_i for every single key
	
	// Current cluster receiving data for
	private int curCluster = 0;
	
	// Clusters update counters: when were the values updated.
	private int lastCurClusterCheck = 0;
	private long lastUpdate = 0;
	
	// Adaptive mechanisms for sampling and dropping
	private boolean adaptiveSampling = false;
	private boolean adaptiveDropping = false;
	
	// Account how many times we have been trying to drop
	private int droppingTimes = 0;
	
	// Spill to disk
	/*private ObjectOutputStream spillout;
	private File tempFile;
	private static final int MAX_NUM_KEYS = 100*1000;*/
	
	private float targetError = 1/100f;
	
	/**
	 * Initialize the date structure to store all the data required for multistage sampling.
	 */
	@Override
	public void setup(Context context) {
		super.setup(context);
		
		if (!precise) {
			// Initialize the clusters
			clusters = new ConcurrentHashMap<String,SparseArray<Double>>();
			
			// Adaptive mechanisms
			Configuration conf = context.getConfiguration();
			adaptiveDropping = conf.getBoolean("mapred.map.approximate.drop.adaptive",   false);
			adaptiveSampling = conf.getBoolean("mapred.input.approximate.skip.adaptive", false);
			targetError      = conf.getFloat("mapred.approximate.error.target", 1/100f); // 1% by default
		}
	}
	
	/**
	 * A thread that checks periodically if there are new clusters.
	 */
	class CheckerThread extends Thread {
		private Context context;
		private boolean done = false;
		
		// How long without receiving keys to process
		private static final int MIN_IDLE_SECONDS = 1; 
		
		public CheckerThread(Context context) {
			this.context = context;
		}
		
		/**
		 * Periodically check if there is an update and check results.
		 */
		@Override
		public void run() {
			while (!done) {
				try {
					synchronized (clusters) {
						// Check the quality if we are 2 seconds without new data
						boolean enoughTime = System.currentTimeMillis()-lastUpdate > MIN_IDLE_SECONDS*1000 && lastCurClusterCheck < curCluster;
						boolean enoughDistance = curCluster > lastCurClusterCheck + 10; // 10 more clusters...
						if (lastUpdate != 0 && (enoughTime || enoughDistance) && !done) {
							// Go ahead and check the current quality of the results
							lastCurClusterCheck = curCluster;
							checkQuality(context, false);
							lastUpdate = 0;
						}
					}
					Thread.sleep(1*1000);
				} catch(Exception e) {
					System.err.println("Error checking results: " + e);
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * We are done.
		 */
		public void setDone() {
			done = true;
		}
	}
	
	/**
	 * This is a wrapper for Context that gets values and clusters them if required.
	 */
	public class ClusteringContextIncr extends ClusteringContext {
		public ClusteringContextIncr(Context context) throws IOException, InterruptedException {
			super(context);
		}
	
		/**
		 * Overwrite of regular write() to capture values and do clustering if needed. If we run precise, pass it to the actual context.
		 */
		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,InterruptedException {
			// For precise, we just forward it to the regular context
			// In this implementation, the marks are always Strings
			if (isPrecise() || !(key instanceof Text)) {
				context.write(key, value);
			// If we don't want precise, we save for multistage sampling
			} else {
				// We have to convert the writtable methods into numbers
				Double res = 0.0;
				if (value instanceof LongWritable) {
					res = new Double(((LongWritable) value).get());
				} else if (value instanceof IntWritable) {
					res = new Double(((IntWritable) value).get());
				} else if (value instanceof DoubleWritable) {
					res = new Double(((DoubleWritable) value).get());
				} else if (value instanceof FloatWritable) {
					res = new Double(((FloatWritable) value).get());
				}
				
				// Extract the actual key
				String keyStr = ((Text) key).toString();
				
				// Parameters start by '\0' to be the first when sorting
				if (keyStr.charAt(0) == MARK_PARAM) {
					// m_i
					if (keyStr.matches(MultistageSamplingHelper.formatToMatch(m_SAMPLED))) {
						// Save the value
						m[curCluster] = res.longValue();
						// Update values depending on m
						updateNValues();
						updateMValues(curCluster);
					// M_i/m_i
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(M_SAMPLED))) {
						// Save the value
						M[curCluster] = res.longValue();
					// t_map
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(t_SAMPLED))) {
						tmap[curCluster] = res.intValue();
						// Update the map time model based on this new value
						updateMapTimeModel();
					// yt_i
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(T_SAMPLED))) {
						yt[curCluster] = res.longValue();
					// standard deviation
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(S_SAMPLED))) {
						s2[curCluster] = res.longValue();
					// Other parameters (marks mainly)
					} else {
						// We don't do anything with the marks, they are just... marks
					}
				// Add to the proper cluster to approximate the results
				} else {
					// Add the value for that cluster
					SparseArray<Double> cluster = clusters.get(keyStr);
					if (cluster == null) {
						// We have to create the cluster structure for a non-existing key
						cluster = new SparseArray<Double>(N);
						clusters.put(keyStr, cluster);
						
						// If we have too many keys, spill them to disk
						/*if (clusters.size() >= MAX_NUM_KEYS) {
							synchronized (clusters) {
								System.out.println("-> Enter spill lock");
								spill();
								System.out.println("<- Leave spill lock");
							}
						}*/
					}
					cluster.set(curCluster, res);
				}
			}
		}
	}
	
	/**
	 * Reduce runner that uses a thread to check the results.
	 * We overwrite the super-class method so we don't have to worry about saving results
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		
		// Precise
		if (isPrecise()) {
			while (context.nextKey()) {
				KEYIN key = context.getCurrentKey();
				reduce(key, context.getValues(), context);
			}
		// Sampling
		} else {
			// Start the checker thread
			CheckerThread checker = new CheckerThread(context);
			checker.start();
			
			// Wrap the Context for clustering
			Context clusteringcontext = new ClusteringContextIncr(context);
			
			// Reduce using approximation
			while (context.nextKey()) {
				// Check if we have a new map output to create a new cluster
				KEYIN key = context.getCurrentKey();
				// The keys within a cluster are sorted, when we break the sorting, we have a new cluster
				if (prevKey != null && prevKey.compareTo(key) > 0) {
					curCluster++;
				}
				// Set the value of the previous key
				if (prevKey == null) {
					prevKey = new Text();
				}
				((Text) prevKey).set((Text)key);
				
				// Calling the actual reduce
				reduce(key, context.getValues(), clusteringcontext);
				
				// Keep track on when we get new keys
				lastUpdate = System.currentTimeMillis();
			}
			
			// We can stop the checker thread
			checker.setDone();
		}
		
		cleanup(context);
	}
	
	/**
	 * We perform the final sampling theory here.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Perform the final estimation and output
		if (!isPrecise()) {
			// Only one reducer
			if (reducerId == 0) {
				// We add the NULLKEY, which would be the worst case
				SparseArray cluster = new SparseArray<Double>(N);
				int worstCluster = 0;
				for (int i=0; i<N; i++) {
					// This is the cluster that brings the highest variance
					if (auxm3[i] > auxm3[worstCluster]) {
						worstCluster = i;
					}
				}
				cluster.set(worstCluster, 1.0);
				clusters.put(NULLKEY, cluster);
			}
			
			// Estimate and output the final results
			checkQuality(context, true);
		}
	}
	
	/**
	 * Send the least popular keys to disk.
	 */
	/*private void spill() throws IOException, InterruptedException {
		///////////////////////////////////////////////////
		Runtime runtime = Runtime.getRuntime();
		float mb = 1024*1024f;
		System.out.println("##### Heap utilization statistics [MB] #####");
		System.out.println("Used Memory:  " + (runtime.totalMemory() - runtime.freeMemory()) / mb);
		System.out.println("Free Memory:  " + runtime.freeMemory() / mb);
		System.out.println("Total Memory: " + runtime.totalMemory() / mb);
		System.out.println("Max Memory:   " + runtime.maxMemory() / mb);
		///////////////////////////////////////////////////
		
		long t0 = System.currentTimeMillis();
		int inisize = clusters.size();
	
		// Keep track of the most popular keys to keep
		PriorityQueue<Double> mostPopularKeys = new PriorityQueue<Double>();
	
		// Track most popular keys
		Iterator<Map.Entry<String,SparseArray<Double>>> it = clusters.entrySet().iterator();
		while (it.hasNext()) {
			// Collect the results for each cluster
			SparseArray<Double> cluster = it.next().getValue();
		
			// Estimate the result for this key
			double[] result = estimateResult(cluster);
			double tauhat   = result[0];
			double interval = result[1];
			
			// Add to the priority queue and trim
			mostPopularKeys.add(tauhat);
		}
		while (mostPopularKeys.size() > MAX_NUM_KEYS/2)  {
			mostPopularKeys.poll();
		}
		
		// Check spill fill
		if (spillout == null) {
			// Create spill file
			tempFile = File.createTempFile("spill",".tmp");
			FileOutputStream filespill = new FileOutputStream(tempFile, true);
			spillout = new ObjectOutputStream (filespill);
			System.out.println("Spill file is " + tempFile);
		}
		
		// Send the least popular to the disk
		it = clusters.entrySet().iterator();
		while (it.hasNext()) {
			// Collect the results for each cluster
			Map.Entry<String,SparseArray<Double>> pair = it.next();
			SparseArray<Double> cluster = pair.getValue();
			
			// Estimate the result for this key
			double[] result = estimateResult(cluster);
			double tauhat   = result[0];
			double interval = result[1];
			
			// Start removing anything that is not popular
			if (tauhat <= mostPopularKeys.peek())  {
				// Save it to disk
				spillout.writeObject(pair);
				// Remove from the clusters
				it.remove();
			}
		}
		spillout.flush();
		
		long fileSize = Math.round(tempFile.length()/(1024*1024));
		
		System.out.println(new Date()+": Too many keys with "+curCluster+" clusters (M/m="+(M[curCluster]/+m[curCluster])+"): " + inisize + " -> " + clusters.size() + " t="+String.format("%.1f", (System.currentTimeMillis()-t0)/1000.0) + " File="+fileSize+"MB");
	}*/
	
	/**
	 * Check the error in the results based on the current sampling.
	 */
	protected void checkQuality(Context context, boolean output) throws IOException, InterruptedException {
		// Track errors
		double maxAbs = 0.0;
		double maxError = 0.0;
		double maxErrorRel = 0.0;
		double maxAbsVal = 0.0;
		String maxAbsKey = null;
		
		long t0 = System.currentTimeMillis();
		// Go over all the keys
		Iterator<Map.Entry<String,SparseArray<Double>>> it = clusters.entrySet().iterator();
		while (it.hasNext()) {
			// Collect the results for each cluster
			Map.Entry<String,SparseArray<Double>> pairs = it.next();
			SparseArray<Double> cluster = pairs.getValue();
			
			// Estimate the result for this key
			double[] result = estimateResult(cluster);
			double tauhat   = result[0];
			double interval = result[1];
			
			// TODO check the error according to the user requirements
			if (tauhat > maxAbsVal) {
			//if (interval > maxError) {
				// Calculate the relative error for the maximum absolute one
				maxAbs = tauhat;
				maxError = interval;
				maxErrorRel = interval/tauhat;
				maxAbsVal = tauhat;
				maxAbsKey = pairs.getKey();
			}
			
			// Output the estimated result
			if (context != null && output) {
				String key = pairs.getKey();
				
				// Right now, we onyl support marks as strings, so...
				KEYOUT outkey = (KEYOUT) new Text(key);
				
				// NULLKEY has actually 0 estimation, the range is the important
				if (key.equals("NULLKEY")) {
					tauhat = 0;
				}
				
				// Get value and transform it into approximate writable
				VALUEOUT outvalue;
				if (IntWritable.class.equals(context.getOutputValueClass())) {
					outvalue = (VALUEOUT) new ApproximateIntWritable((int) tauhat, interval);
				} else if (LongWritable.class.equals(context.getOutputValueClass())) {
					outvalue = (VALUEOUT) new ApproximateLongWritable((long) tauhat, interval);
				} else {
					outvalue = (VALUEOUT) new ApproximateLongWritable((long) tauhat, interval);
				}
				
				// Actual and final output
				context.write(outkey, outvalue);
			}
		}
		
		// We can change the sampling ratio dynamically
		String extraMsg = "";
		if (adaptiveSampling) {
			// Calculate the maximum sampling ratio for the current key
			int[] result = getMaxSamplingRatio(clusters.get(maxAbsKey), targetError);
			
			// Extract results
			int bestT = result[0];
			int bestN = result[1];
			int maxSamplingRatio = result[2];
			
			if (bestN > n+1) {
				// Set the new parameter for the new maps
				context.getCounter("Multistage Sampling", "Tauhat "+Integer.toString(reducerId)).setValue((long) maxAbsVal);
				context.getCounter("Multistage Sampling", "Time "+Integer.toString(reducerId)).setValue(bestT);
				context.getCounter("Multistage Sampling", "Clusters "+Integer.toString(reducerId)).setValue(bestN);
				context.getCounter("Multistage Sampling", "SamplingRatio "+Integer.toString(reducerId)).setValue(maxSamplingRatio);
				// Output message
				extraMsg += " N="+bestN+" M/m="+(M[curCluster]/m[curCluster])+"->"+maxSamplingRatio;
			} else {
				// If we achieve have almost achieved the goal (bestN==n), we don't tune M/m anymore
				// The problem was that by default we would set M/m to the highest number and this breaks the following iterations.
				extraMsg += " N="+bestN+" M/m="+(M[curCluster]/m[curCluster]);
			}
		}
		
		// We can start dropping dynamically
		if (adaptiveDropping) {
			if (maxErrorRel < targetError) {
				droppingTimes++;
				if (droppingTimes > 2) {
					System.out.format("We have %.1f%% [<%.1f%%] error, let's drop!\n", maxErrorRel*100, targetError*100);
					context.setStatus("dropping");
				}
			} else {
				// Reset back to default
				droppingTimes = 0;
				context.setStatus("reduce");
			}
		}
		
		// Info message for debugging
		extraMsg = "["+extraMsg.trim()+"]";
		System.out.format("%s: %.1fs %d/%d max error for \"%s\" is %s+/-%s (+/-%.2f%%) Keys=%d %s\n",
			new Date(),
			(System.currentTimeMillis()-t0)/1000.0,
			n, N,
			maxAbsKey,
			MultistageSamplingHelper.toNumberString(maxAbs),
			MultistageSamplingHelper.toNumberString(maxError),
			maxErrorRel*100,
			clusters.size(),
			extraMsg);
	}
	
	/**
	 * Get the estimated result for the argument.
	 * @param cluster Clusters for a key.
	 * @return [tauhat, tscore*setauhat]
	 */
	protected double[] estimateResult(SparseArray<Double> cluster) throws IOException, InterruptedException {
		// Start estimating
		double[] yhat = new double[n];//-emptyClusters];
		double var2 = 0.0;
		
		// This thing about splitting in two ways is very ugly but... performance my friend.
		if (cluster.isSparse()) {
			// Sparse matrix, so we can skip a lot of computation
			synchronized (cluster.sparse) {
				// Go over the positions different than 0
				for (SparseArray<Double>.SparseNode<Double> aux : (LinkedList<SparseArray<Double>.SparseNode<Double>>) cluster.sparse.clone()) {
					int i = aux.p;
					if (i < yhat.length) { // We could be checking newer values, stick to what we knew at the beginning
						double yi = aux.v;
						// Estimate the results in each cluster
						yhat[i] = auxm1[i] * yi;
						// Estimate variance
						if (s2[i] >= 0) {
							var2 += auxm3[i] * s2[i];
						} else if (s2[i] == -3) {
							var2 += auxm3[i] * (0.1*yi);
						} else {
							// For boolean variables
							double pi = 1.0*yi/m[i];
							if (pi > 1 || m[i]==0) {
								System.err.println("Error! y["+i+"]="+yi+" m["+i+"]="+i+" p["+i+"]="+pi);
							} else if (m[i] > 1) { // Otherwise, the variance is 0
								// Variance to the secondary
								var2 += auxm2[i] * pi * (1.0-pi);
							}
						}
					}
				}
			}
		// Now we go over everybody
		} else {
			synchronized (cluster.array) {
				// Go over all the positions
				int i = 0;
				for (Double auxyi : cluster.array) {
					// Check if there is a value, otherwise, we can leave the default yhat[i]=0.0
					if (auxyi != null) { // auxyi==null => y_i=0
						double yi = auxyi;
						// Estimate the results in each cluster
						yhat[i] = auxm1[i] * yi;
						// Estimate variance
						if (s2[i] >= 0) {
							var2 += auxm3[i] * s2[i];
						} else if (s2[i] == -3) {
							var2 += auxm3[i] * (0.1*yi);
						} else {
							// For boolean variables
							double pi = 1.0*yi/m[i];
							if (pi > 1 || m[i]==0) {
								System.err.println("Error! y["+i+"]="+yi+" m["+i+"]="+i+" p["+i+"]="+pi);
							} else if (m[i] > 1) { // Otherwise, the variance is 0
								// Variance to the secondary
								var2 += auxm2[i] * pi * (1.0-pi);
							}
						}
					}
					i++;
					// If we pass this point we would be checking newer values
					if (i >= yhat.length) {
						break;
					}
				}
			}
		}
		// This is the regular implementation but we unfolded it for performance
		/*for (int i=0; i<n; i++) {
			Double auxyi = cluster.get(i);
			if (auxyi != null) {
				// y_i
				double yi = auxyi;
				// yt_i
				double yti = 0.0;
				if (clusterT[i] != null) {
					yti = clusterT[i];
				} else if (clusterM[i] != null) {
					yti = clusterM[i];
				}
				// Estimate the results in each cluster
				yhat[i] = auxm1[i] * yi;
				// Estimate proportions
				double pi = 1.0*yi/yti;
				// Variance to the secondary
				var2 += auxm2[i] * pi * (1.0-pi);
			} else {
				// If yi==0 we can say that the rest is 0
				yhat[i] = 0.0;
			}
		}*/
		var2 = Nn * var2; // N/n
		
		// Estimate the total number
		double tauhat = Nn * MultistageSamplingHelper.sum(yhat); // N/n
		
		// Calculate first variance variance
		double var1 = 0.0;
		if (n < N) {
			var1 = NNnn * MultistageSamplingHelper.var(yhat);
		}
		
		// Calculate standard error
		double setauhat = 0.0;
		if (var1 < 0 || var2 < 0) {
			// This shouldn't happen, this most likely means overflow
			setauhat = Double.MAX_VALUE;
		} else {
			double vartauhat = var1 + var2;
			setauhat = Math.sqrt(vartauhat);
		}
		
		return new double[] {tauhat, tscore*setauhat};
	}
	
	/**
	 * Get the maximum sampling ratio to get an error. It uses a sparse cluster as this is for increment.
	 * @param cluster The value containing the data to evaluate.
	 * @param targetError The maximum percentage error allowed.
	 * @return [n, M/m]
	 */
	protected int[] getMaxSamplingRatio(SparseArray<Double> cluster, float targetError) {
		int n1 = n;
	
		// Calculate the average size (M) and variance (s^2)
		double Mavg = 0.0;
		double Mmax = 0.0;
		for (int i=0; i<n1; i++) {
			// Number of samples
			Mavg += M[i];
			if (M[i] > Mmax) {
				Mmax = M[i];
			}
		}
		Mavg = Mavg/n1;
		
		// Get the variation parameters for the maximum value
		double[] yhat = new double[n1];
		double var2 = 0.0;
		double s2avg = 0.0;
		// We only do this once in a while, so we can iterate over all the clusters for this value
		for (int i=0; i<n1; i++) {
			// Current value
			Double yi = cluster.get(i);
			if (yi==null) {
				yi = 0.0;
			}
			// Estimate the results for each cluster
			yhat[i] = auxm1[i] * yi;
			// Estimate variation
			if (s2[i] >= 0) {
				var2 += auxm3[i] * s2[i];
				// Calculate average value
				s2avg += s2[i];
			} else if (s2[i] == -3) {
				var2 += auxm3[i] * (0.1*yi);
				// Calculate average value
				s2avg += 0.1*yi;
			} else {
				// For boolean variables
				double pi = 1.0*yi/m[i];
				if (pi > 1 || m[i]==0) {
					System.err.println("Error! y["+i+"]="+yi+" m["+i+"]="+m[i]+" p["+i+"]="+pi);
				} else if (m[i]==1) {
					// The variance is 0
				} else {
					// Variance to the secondary
					var2 += auxm2[i] * pi * (1.0-pi);
					// Calculate average value
					s2avg += (m[i]/(m[i]-1.0)) * pi * (1.0-pi);
				}
			}
		}
		s2avg = s2avg/n1;
		
		// Estimate the total number
		double tauhat = Nn * MultistageSamplingHelper.sum(yhat); // N/n
		
		// We use the optimization approach
		int[] result = minExecutionTime(n1, tauhat, MultistageSamplingHelper.var(yhat), var2, (int) Math.round(Mavg), s2avg, targetError);
		int bestT = result[0];
		int bestN = result[1];
		int bestR = result[2]; // Maximum sampling ratio
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Run "+bestN+" (n="+(bestN+n1)+") more maps with M/m=" + bestR+" in "+bestT+" seconds");
			
			// Estimate final errors
			double auxtscore = MultistageSamplingHelper.getTScore(N-1, 0.95);
			double interval = auxtscore*Math.sqrt(n1*var2/N);
			
			/////////////////////////////////////////////////////////
			// Debugging for a bug that shows up once in a blue moon
			// Last seen on 30/05/2014
			/////////////////////////////////////////////////////////
			if (Double.isNaN(interval)) {
				System.err.println("The interval is not a number!");
				System.err.println("  interval="+interval);
				System.err.println("  var2="+var2);
				System.err.println("  N="+N);
				System.err.println("  n1="+n1);
			}
			/////////////////////////////////////////////////////////

			// Follow the progress of the approximation
			LOG.debug(String.format("If we continue like this, the result would be: %s +/- %s (+/-%.2f%%) [<%.2f%%] => M/m=%d",
				MultistageSamplingHelper.toNumberString(tauhat),
				MultistageSamplingHelper.toNumberString(interval),
				100.0*interval/tauhat,
				100*targetError,
				//(int) Math.floor(1.0*M[curCluster]/m[curCluster]),
				bestR));
		}
		
		return new int[] {bestT, bestN, bestR};
	}
}



	/**
	 * Check the error in the results based on the current sampling.
	 */
	/*protected void checkQualityComplete(Context context, boolean output) throws IOException, InterruptedException {
		long t0 = System.currentTimeMillis();
	
		double maxAbs = 0.0;
		double maxError = 0.0;
		double maxErrorRel = 0.0;
		
		// Multistage sampling
		int N = numCluster;
		int n = curCluster+1;
		
		// Calculate populations in each cluster
		long[] mi = new long[n];
		long[] Mi = new long[n];
		for (int i=0; i<n; i++) {
			if (clusterM[i] != null) {
				mi[i] = clusterM[i].longValue();//clusterLines[i];
			} else {
				// If not defned, we add all the values
				mi[i] = 0;
				for (Double v : clusters.get(i).values()) {
					mi[i] += v.longValue();
				}
			}
			Mi[i] = mi[i]*skipSamples; // This is an approximation based on the sampling ratio
		}
		
		// Get the t-score for the current distribution
		double tscore = getTScore(n-1, 0.95);
		
		// Go over all the keys
		for (Object key : clusters.keySet()) {
			// Collect the results for each cluster
			double[] yi  = new double[n];
			double[] yti = new double[n];
			for (int i=0; i<n; i++) {
				yi[i] = 0;
				try {
					yi[i] = clusters.get(key).get(i);
				} catch(Exception e) {}
				// y_t
				if (clusterT[i] != null) {
					yti[i] = clusterT[i];
				} else {
					yti[i] = clusterM[i];
				}
			}

			// Estimate the results in each cluster
			double[] yhat = new double[n];
			for (int i=0; i<n; i++) {
				yhat[i] = (1.0*Mi[i]/mi[i])* yi[i];
			}
			
			// Estimate the total number
			double tauhat = (1.0*N/n)*sum(yhat);
			
			// Calculate the errors
			// Estimate the global deviation
			double su2 = var(yhat);
		
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
			if (n == N) {
				var1 = 0.0;
			}
			double var2 = 0.0;
			for (int i=0; i<n; i++) {
				var2 += 1.0*(Mi[i]*(Mi[i]-mi[i])*si2[i])/mi[i];
			}
			var2 = (1.0*N/n)*var2;
			
			double vartauhat = var1 + var2;
			Double setauhat = Math.sqrt(vartauhat);
			if (setauhat.isNaN()) { 
				setauhat = Double.MAX_VALUE;
			}
			
			// TODO select error according to the user requirements
			// Calculate the maximum relative error
			if (tscore*setauhat > maxError) {
				maxAbs = tauhat;
				maxError = tscore*setauhat;
				maxErrorRel = 100.0*tscore*setauhat/tauhat;
			}
			
			// Output the estimation
			if (output) {
				// Get key and transform it into writable
				KEYOUT outkey;
				if (key instanceof Long) {
					outkey = (KEYOUT) new LongWritable((Long)key);
				} else if (key instanceof Integer) {
					outkey = (KEYOUT) new IntWritable((Integer)key);
				} else if (key instanceof Double) {
					outkey = (KEYOUT) new DoubleWritable((Double)key);
				} else if (key instanceof Float) {
					outkey = (KEYOUT) new FloatWritable((Float)key);
				} else {
					outkey = (KEYOUT) new Text((String)key);
				}
				
				// Get value and transform it into approximate writable
				VALUEOUT outvalue;
				if (IntWritable.class.equals(context.getOutputValueClass())) {
					outvalue = (VALUEOUT) new ApproximateIntWritable((int) tauhat, tscore*setauhat);
				} else if (LongWritable.class.equals(context.getOutputValueClass())) {
					outvalue = (VALUEOUT) new ApproximateLongWritable((long) tauhat, tscore*setauhat);
				} else {
					outvalue = (VALUEOUT) new ApproximateLongWritable((long) tauhat, tscore*setauhat);
				}
				
				// Actual output
				context.write(outkey, outvalue);
			}
		}
		// Info message
		System.out.format("%s: %.1fs %d/%d max error %s+/-%s (+/-%.2f%%) Keys=%d\n", new Date(), (System.currentTimeMillis()-t0)/1000.0, n, N, toNumberString(maxAbs), toNumberString(maxError), maxErrorRel, clusters.size());
	}*/
