package org.apache.hadoop.mapreduce.approx.multistage;

import java.io.IOException;

import java.util.Iterator;
import java.util.Date;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.commons.math.distribution.TDistribution;
import org.apache.commons.math.distribution.TDistributionImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.util.Progressable;

import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;
import org.apache.hadoop.mapreduce.approx.ApproximateIntWritable;

import org.apache.log4j.Logger;

/**
 * Perform multistage sampling.
 * It gets the data from the reducers and performs sampling on the fly.
 * The data needs to come properly sorted.
 * @author Inigo Goiri
 */
public abstract class MultistageSamplingReducer<KEYIN extends Text,VALUEIN,KEYOUT,VALUEOUT extends WritableComparable> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger(MultistageSamplingReducer.class);
	
	public static final char MARK_PARAM = '\0';
	// Formats that the multistage clustering reducer recognizes. It is [NAME][mapId]-[reduceId]
	public static final String m_SAMPLED = MARK_PARAM + "m%d-%d"; // number of samples in cluster [this is the one that gets later]
	public static final String M_SAMPLED = MARK_PARAM + "M%d-%d"; // population size (M) [We need M to arrive earlier than m]
	public static final String T_SAMPLED = MARK_PARAM + "T%d-%d"; // yt_i
	public static final String S_SAMPLED = MARK_PARAM + "S%d-%d"; // Standard deviation
	public static final String t_SAMPLED = MARK_PARAM + "t%d-%d"; // Time to run the map
	// We need two additional marks to guarrantee we identify clusters properly, the name doesn't really matter (neither the order)
	public static final String CLUSTERINI = MARK_PARAM + "I%d-%d";
	public static final String CLUSTERFIN = MARK_PARAM + "F%d-%d";
	
	public static final String NULLKEY = "NULLKEY";
	
	// If we are doing the precise or execution
	protected boolean precise = false;
	
	// Keep track of what was the last key
	protected String prevKey = null;
	
	protected int reducerId = 0;
	
	// Multistage sampling
	protected int N = 0;
	protected int n = 0;
	
	// Current values
	protected double[] y;
	// Value for multistage sampling in-place
	protected long[] m; // Number of samples in cluster i
	protected long[] M; // Population size (Mi)
	// Value for multistage sampling in-place
	protected double[] yt;
	protected double[] s2;
	
	// Precomputed values
	protected double Nn = 0.0;
	protected double NNnn = 0.0;
	protected double[] auxm1; // M/m
	protected double[] auxm2; // M(M-m)/m-1
	protected double[] auxm3; // M(M-m)/m
	
	// Score based on T-student distribution for estimating the range
	protected double tscore = 1.96; // Default is 95% for high n
	
	// Profile map length
	protected int[] tmap;
	
	// Model for map length
	protected double timeModelTr = 8E-6;
	protected double timeModelTp = 8E-6;
	protected double timeModelT0 = 3;
	
	// The maximum sampling ratio that makes sense; after this it doesn't give much
	private static final int MAX_SAMPLING = 10*1000;
	
	/**
	 * Initialize all the variables needed for multistage sampling.
	 */
	@Override
	public void setup(Context context) {
		// Check if we are precise or not
		Configuration conf = context.getConfiguration();
		precise = conf.getBoolean("mapred.job.precise", false);
		
		reducerId = context.getTaskAttemptID().getTaskID().getId();
		
		// If we are approximating, we use the rest
		if (!precise) {
			// The total number of clusters
			N  = conf.getInt("mapred.map.tasks", -1);
			
			// Default value for the sampling ratio
			//int skipSamples = conf.getInt("mapred.input.approximate.skip", 1);
			
			// Initialize values we will use frequently
			y  = new double[N];
			m  = new long[N];
			M  = new long[N];
			yt = new double[N];
			s2 = new double[N];
			tmap = new int[N];
			for (int i=0; i<N; i++) {
				// -1 means we have not received anything for this cluster
				m[i]  = -1;
				M[i]  = -1;
				yt[i] = -1;
				s2[i] = -1;
				tmap[i] = -1;
			}
			
			// Structure for precomputed values
			auxm1 = new double[N]; // skipSamples = 1.0 * Mi / mi;
			auxm2 = new double[N]; // (Mi*(Mi-mi)) / (mi-1.0);
			auxm3 = new double[N]; // (Mi*(Mi-mi)) / mi;
		}
	}
	
	/**
	 * This is a wrapper for Context that gets values and clusters them if required.
	 */
	public class ClusteringContext extends Context {
		protected Context context;
		
		/**
		 * We need to store the wrapped context to forward everything.
		 */
		public ClusteringContext(Context context) throws IOException, InterruptedException {
			// This is just a wrapper, so we don't create anything
			super(context.getConfiguration(), context.getTaskAttemptID(), null, null, null, null, context.getOutputCommitter(), null, (RawComparator<KEYIN>) context.getSortComparator(), (Class<KEYIN>) context.getMapOutputKeyClass(), (Class<VALUEIN>) context.getMapOutputValueClass());

			// Save the wrapped context
			this.context = context;
		}
		
		/**
		 * Update "n" and "N" values
		 */
		protected void updateNValues() {
			// Update "n" and "N" values
			n++;
			Nn = 1.0*N/n;
			NNnn = 1.0*N*(N-n)/n;
			
			// Update t-score
			tscore = MultistageSamplingHelper.getTScore(n-1, 0.95);
		}
		
		/**
		 * Update the m/M related values
		 */
		protected void updateMValues(int clusterId) {
			if (m[clusterId] > 0) {
				//long Mi = m[clusterId]*Mm[clusterId];
				auxm1[clusterId] = 1.0 * M[clusterId]/m[clusterId]; // 1.0 * Mi / mi;
				if (m[clusterId] > 1) {
					auxm2[clusterId] = 1.0 * (M[clusterId]*(M[clusterId]-m[clusterId])) / (m[clusterId]-1.0); // Mi(Mi-mi)/(mi-1)
				} else {
					auxm2[clusterId] = 0; // This is used to calculate the current variance and with 1 there is no variance
				}
				auxm3[clusterId] = 1.0 * (M[clusterId]*(M[clusterId]-m[clusterId])) /  m[clusterId];      // Mi(Mi-mi)/mi
			} else {
				auxm1[clusterId] = 0;
				auxm2[clusterId] = 0;
				auxm3[clusterId] = 0;
				
			}
			/* else {
				emptyClusters++;
				System.err.println("We got an empty sample at cluster i="+clusterId+"! Empty clusters="+emptyClusters);
			}*/
		}
		
		/**
		 * Estimate and save the current result.
		 */
		public void saveCurrentResult() throws IOException,InterruptedException {
			double[] result = estimateCurrentResult();
			double tauhat   = result[0];
			double interval = result[1];
			
			context.write((KEYOUT) new Text(prevKey), (VALUEOUT) new ApproximateLongWritable((long) tauhat, interval));
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
				byte[] aux = keyStr.getBytes();
				String origKey = new String(aux, 0, aux.length-2);
				
				// We changed the key and the previous one wasn't a parameter, write it!
				if (!origKey.equals(prevKey) && prevKey != null && prevKey.charAt(0) != MARK_PARAM) {
					saveCurrentResult();
				}
				
				// Parameters start by '\0' to be the first when sorting
				if (keyStr.charAt(0) == MARK_PARAM) {
					// The parameters always have format: [NAME][MAPID/CLUSTERID]-[REDUCEID]
					int clusterId = Integer.parseInt(keyStr.substring(2, keyStr.lastIndexOf("-"))); // \0M[mapId]-[reduceId]
					// m_i
					if (keyStr.matches(MultistageSamplingHelper.formatToMatch(m_SAMPLED))) {
						// Save the value
						m[clusterId] = res.longValue();
						// Update values depending on m
						updateNValues();
						updateMValues(clusterId);
					// M_i/m_i
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(M_SAMPLED))) {
						// Save the value
						M[clusterId] = res.longValue();
						// This is the one that gets first, so we don't need to update
					// t_map
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(t_SAMPLED))) {
						tmap[clusterId] = res.intValue();
						// Update the map time model based on this new value
						updateMapTimeModel();
					// yt_i
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(T_SAMPLED))) {
						yt[clusterId] = res.longValue();
					// standard deviation
					} else if (keyStr.matches(MultistageSamplingHelper.formatToMatch(S_SAMPLED))) {
						s2[clusterId] = res.longValue();
					// Other parameters (marks mainly)
					} else {
						// We don't do anything with the marks, they are just... marks
					}
				// Regular values
				} else {
					// Calculate the map this key comes from, it is the last two bytes
					int clusterId = 128*aux[aux.length-2]+aux[aux.length-1]; // Last two characters
					// Save the value
					y[clusterId] = res;
				}
				prevKey = origKey;
			}
		}

		// We overwrite these method to avoid problems, ideally we would forward everything
		public void getfirst() throws IOException { }
		
		public float getProgress() {
			return context.getProgress();
		}
		
		public void progress() {
			context.progress();
		}
		
		public void setStatus(String status) {
			context.setStatus(status);
		}
		
		public Counter getCounter(Enum<?> counterName) {
			return context.getCounter(counterName);
		}
		
		public Counter getCounter(String groupName, String counterName) {
			return context.getCounter(groupName, counterName);
		}
	}
	
	/**
	 * Reduce runner that uses a thread to check the results.
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
			// We wrap the context to capture the writing of the keys
			ClusteringContext clusteringContext = getClusteringContext(context);
			
			while (context.nextKey()) {
				KEYIN key = context.getCurrentKey();
				// Note that we pass the parameters over the reducer too
				reduce(key, context.getValues(), clusteringContext);
			}
			
			// We need to treat the last key (not parameters)
			if (prevKey != null && prevKey.charAt(0) != MARK_PARAM) {
				clusteringContext.saveCurrentResult();
			}
		}
		
		cleanup(context);
	}
	
	/**
	 * Wrapper to create a new clustering context according to the user specs.
	 * It returns a regular context that outputs the estimated result.
	 */
	protected ClusteringContext getClusteringContext(Context context) throws IOException, InterruptedException {
		return new ClusteringContext(context);
	}
	
	/**
	 * To finish we estimate the error of keys that didn't show up because of sampling.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		if (!isPrecise()) {
			// Only one reducer
			if (reducerId == 0) {
				// We add a NULLKEY to estimate the range of everything we didn't see because of sampling
				prevKey = NULLKEY;
				
				// Find the worst cluster
				int worstCluster = 0;
				for (int i=0; i<N; i++) {
					// This is the cluster that brings the highest variance
					if (auxm3[i] > auxm3[worstCluster]) {
						worstCluster = i;
					}
				}
				// We simulate we have one there
				y[worstCluster] = 1.0;
				
				// Calculate the error for the null key
				double[] result = estimateCurrentResult();
				double tauhat   = result[0];
				double interval = result[1];
				
				// Let's output the value
				context.write((KEYOUT) new Text(prevKey), (VALUEOUT) new ApproximateLongWritable((long) tauhat, interval));
			}
		}
	}
	
	/**
	 * Check if we run the job precisely.
	 */
	public void setPrecise() {
		this.precise = true;
	}
	
	public boolean isPrecise() {
		return this.precise;
	}
	
	/**
	 * Get the estimated result for the current key.
	 * @return [tauhat, tscore*setauhat]
	 */
	protected double[] estimateCurrentResult() {
		return estimateCurrentResult(true);
	}
	
	protected double[] estimateCurrentResult(boolean reset) {
		// Estimate the result
		double[] yhat = new double[n];
		double var2 = 0.0;
		
		// Process every cluster
		int j=0;
		for (int i=0; i<N; i++) {
			// Check if this is a received cluster
			if (M[i] >= 0){
				// We can skip a few because of dropping and the result for yi==0 is 0
				if (m[i] > 0 && y[i] > 0) {
					// Estimate the results in each cluster
					yhat[j] = auxm1[i] * y[i];
					// Calculate variance
					if (s2[i] >= 0) {
						// Based on standard deviation from the map
						var2 += auxm3[i] * s2[i];
					} else if (s2[i] == -3) {
						// Worst case is a variance of all the values: si2 = 10% of yi
						var2 += auxm3[i] * (0.1*y[i]);
					} else if (m[i] > 1) { // m==1 has no variance
						// Estimate proportions
						double pi = 1.0*y[i]/m[i];
						// Variance to the secondary
						var2 += auxm2[i] * pi * (1.0-pi);
					}
					// Reset yi=0 for the next key
					if (reset) {
						y[i] = 0.0;
					}
				}
				j++;
			}
		}
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
			setauhat = Double.MAX_VALUE;
		} else {
			double vartauhat = var1 + var2;
			setauhat = Math.sqrt(vartauhat);
		}
		
		// The null key is 0 as we actually don't see it
		if (prevKey.equals("NULLKEY")) {
			tauhat = 0.0;
		}
		
		return new double[] {tauhat, tscore*setauhat};
	}
	
	/**
	 * Get the sampling ratio (M/m) for a maximum relative error (<code>targetError</code>) when n=N.
	 * We don't use it right now.
	 */
	protected int minExecutionTime(int n1, double tauhat, double var2, int Mavg, double s2avg, float targetError) {
		/*
		t_score*sqrt(var2)
		------------------ < targetError
		      tauhat
		
		 M    (target*tauhat/t)^2 - var2
		--- < -------------------------- + 1
		 m         (N/n) (N-n) M s^2
		*/
		double auxtscore = MultistageSamplingHelper.getTScore(N-1, 0.95);
		double maxSamplingRatio = (Math.pow(targetError*tauhat/auxtscore, 2) - var2) / ((N-n1) * Mavg * s2avg) + 1;
		
		if (maxSamplingRatio < 1) {
			maxSamplingRatio = 1.0;
		}
		
		if (maxSamplingRatio > MAX_SAMPLING) {
			maxSamplingRatio = MAX_SAMPLING;
		}
		
		return (int) Math.round(maxSamplingRatio);
	}
	
	/**
	 * Get the number of cluster (n) and the sampling ratio (M/m) for a maximum relative error (<code>targetError</code>).
	 */
	protected int[] minExecutionTime(int n1, double tauhat, double s2, double var2, int Mavg, double s2avg, float targetError) {
		// Search the n2 where we start getting our target error
		int nini = 0;
		int nfin = N+1-n1;
		while (nfin-nini > 1) {
			int nmid = (nfin+nini)/2;
			double err = getRelativeError(tauhat, s2, var2, n1, nmid, Mavg, s2avg, 1)/100.0;
			if (err > targetError) {
				nini = nmid;
			} else {
				nfin = nmid;
			}
		}
		// The n where we start getting results
		int startn2 = nini;
		
		// Search R=M/m where we minimize the error
		double bestT = Double.MAX_VALUE;
		int bestR = 1;
		int bestN = startn2;
		for (int n2=startn2; n2<N-n1; n2+=1) {
			if (getTotalTime(n2, Mavg, MAX_SAMPLING) > bestT) {
				// No point on going further than this, after this, everything is worse
				break;
			} else {
				// Search the maximum sampling ration
				int rini = 1;
				int rfin = MAX_SAMPLING+1; // More sampling than this is not very useful as far as we know
				while (rfin-rini > 1) {
					int rmid = (rfin+rini)/2;
					double err = getRelativeError(tauhat, s2, var2, n1, n2, Mavg, s2avg, rmid)/100.0;
					if (err > targetError) {
						rfin = rmid;
					} else {
						rini = rmid;
					}
				}
				int maxR = rini;
				double t = getTotalTime(n2, Mavg, maxR);
				if (t<bestT) {
					bestT = t;
					bestR = maxR;
					bestN = n2;
				}
			}
		}
		
		/*
		// We could've used this instead of a search but it's not precise... probably my incompetence
		double var1 = 1.0*N*(N-n1-n2)*s2/(n1+n2);
		double var2past = (1.0*N/(n1+n2)) * var2;
		
		return 1 + ((1.0*(n1+n2)) / (1.0*N*n2*avgM*avgS2)) * (Math.pow(targetError*tauhat/auxtscore, 2) - var1 - (1.0*N/(n1+n2))*var2past);
		*/
		
		return new int[] {(int) Math.round(getTotalTime(bestN, Mavg, bestR)), bestN+n1, bestR};
	}
	
	/**
	 * Calculate the relative error for a value. It can also predict the error in the future iterations (n2).
	 * @param tauhat Current total estimation.
	 * @param s2 Current primary variance.
	 * @param var2 Variance for the past clusters.
	 * @param n1 Number of maps we had run.
	 * @param n2 Number of new maps we will run.
	 * @param mapM Average size of a cluster.
	 * @param mapR Sampling ratio for the new maps.
	 * @param mapS2 Variance for the past cluster.
	 */
	protected double getRelativeError(double tauhat, double s2, double var2, int n1, int n2, int avgM, double avgS2, double r) {
		// Calculate the percentage error with this values
		double auxtscore = MultistageSamplingHelper.getTScore(n1+n2-1, 0.95); // 95% confidence
			
		double var1 = 1.0*N*(N-n1-n2)*s2/(n1+n2);
		double var2past = var2;
		double var2future = 1.0 * n2 * avgM * (r-1.0) * avgS2;
			
		double var = var1 + (1.0*N/(n1+n2)) * (var2past + var2future);
		double setauhat = Math.sqrt(var);
		
		return (100.0*auxtscore*setauhat/tauhat);
	}
	
	/**
	 * Get the execution time to run the rest of maps with this setup.
	 */
	protected double getTotalTime(int nmap, int mapM, int mapR) {
		return nmap*getMapTime(mapM, mapR);
	}
	
	/**
	 * Estimate the running time of a map.
	 * @param mapM M
	 * @param mapR M/m
	 * @return Runtime in seconds
	 */
	protected double getMapTime(int mapM, int mapR) {
		double t = 0.0;
		t += timeModelT0; // Hadoop overhead (t_0)
		t += timeModelTr * mapM; // t_r
		t += timeModelTp * (1.0*mapM/mapR); // t_p
		return t;
	}
	
	/**
	 * We learn the model for a map duration.
	 */
	protected void updateMapTimeModel() {
		// Get the input data for the model
		double[]   t = new double[n]; // Y
		double[][] x = new double[n][3]; // X
		int noSampling = 0;
		int j=0;
		for (int i=0; i<N; i++) {
			// If we have info for this cluster
			if (M[i] >= 0) {
				// Output
				t[j] = tmap[i];
				// Input
				x[j][0] = M[i]; // t_r 
				x[j][1] = m[i]; // t_p
				x[j][2] = 1.0;  // t0
				// Account for no sampling values
				if (x[j][0] == x[j][1]) {
					noSampling++;
				}
				j++;
			}
		}
		
		// Check if we have enough to make an OK model
		double NO_SAMPLING_THRESHOLD = 66/100.0; // 66%
		if (t.length > 0 && noSampling > NO_SAMPLING_THRESHOLD*t.length) {
			// We manually add points with high sampling and we assume the time is half of the precise
			for (int i=0; i < t.length && noSampling > NO_SAMPLING_THRESHOLD*t.length; i++) {
				if (x[i][0] == x[i][1]) {
					// Half of the time
					t[i] = t[i]/2.0;
					x[i][1] = 1.0;
					noSampling--;
				}
			}
		}
		
		try {
			// Perform regression
			boolean model = false;
			if (t.length > 3) {
				OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();
				reg.newSampleData(t, x);
				
				// Perform regression
				double[] b = reg.estimateRegressionParameters();
				if (b.length == 3 && b[0]>0 && b[1]>0 && b[2]>0) {
					timeModelTr = b[0];
					timeModelTp = b[1];
					timeModelT0 = b[2];
					model = true;
				}
			}
			
			// We use a simple model
			if (!model) {
				double Mavg = 0.0;
				double mavg = 0.0;
				double tavg = 0.0;
				for (int i=0; i<n; i++) {
					Mavg += 1.0*x[i][0]/n;
					mavg += 1.0*x[i][1]/n;
					tavg += 1.0*t[i]/n;
				}
				timeModelT0 = 3;
				double tt = (tavg-timeModelT0)/(Mavg+mavg);
				timeModelTr = tt;
				timeModelTp = tt;
				//double tt = (tavg-timeModelT0)/Mavg;
				// Equal weight for t_r and t_p
				//timeModelTr = tt/2.0;
				//timeModelTp = tt/2.0;
			}
		} catch (Exception e) {
			System.err.println("We got a weird map duration model with n="+n+":");
			e.printStackTrace();
		}
		
		LOG.debug("Map time model: t_map = M x " + timeModelTr + " + m x " + timeModelTp + " + " + timeModelT0);
	}
}
