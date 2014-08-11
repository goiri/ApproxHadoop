package org.apache.hadoop.mapreduce.approx.multistage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Reducer that performs a pilot sample.
 * It evaluates the variance in the clusters to check how many samples to take.
 */
public class MultistageSamplingPilot extends MultistageSamplingReducer<Text,LongWritable,Text,LongWritable> {
	private float targetError = 1/100f; // 1% target by default

	/**
	 * Initialize all the variables needed for multistage sampling.
	 */
	@Override
	public void setup(Context context) {
		super.setup(context);
		
		// Get attributes related to sampling
		Configuration conf = context.getConfiguration();
		targetError = conf.getFloat("mapred.approximate.error.target", 1/100f); // 1% by default
	}

	/**
	 * Estimate of the sampling ratio for the current key.
	 * It extends the regular clustering context but calculates the best sampling ratio.
	 */
	public class PilotContext extends ClusteringContext {
		private double maxTauhat = 0.0;
		private int maxSamplingRatio = 0;

		public PilotContext(Context context) throws IOException, InterruptedException {
			super(context);
		}

		/**
		 * Instead of saving the estimation for the value, it calculates the best sampling ratio.
		 */
		@Override
		public void saveCurrentResult() {
			// To speed things up, we first calculate the estimated value and then see if we should use it to calculate the sampling ratio
			double[] result = estimateCurrentResult(false);
			double tauhat   = result[0];
			double interval = result[1];
		
			// Check if this is the largest key so far
			if (tauhat > maxTauhat) {
				maxTauhat = tauhat;
				
				// Calculate the sampling ratio now
				int[] result2 = getMaxSamplingRatio(targetError);
				int bestT         = result2[0];
				int bestN         = result2[1];
				int samplingRatio = result2[2];

				System.out.format("\"%s\" (%s+/-%s) requires %d maps at M/m=%d in %d seconds\n",
					prevKey,
					MultistageSamplingHelper.toNumberString(tauhat),
					MultistageSamplingHelper.toNumberString(interval),
					bestN, samplingRatio, bestT);
				// Save if it's the maximum sampling ratio
				if (samplingRatio > maxSamplingRatio) {
					maxSamplingRatio = samplingRatio;
					context.getCounter("Multistage Sampling", "Tauhat "+Integer.toString(reducerId)).setValue((long) tauhat);
					context.getCounter("Multistage Sampling", "Time "+Integer.toString(reducerId)).setValue(bestT);
					context.getCounter("Multistage Sampling", "Clusters "+Integer.toString(reducerId)).setValue(bestN);
					context.getCounter("Multistage Sampling", "SamplingRatio "+Integer.toString(reducerId)).setValue(maxSamplingRatio);
				}
			}
			
			// We have to reset y
			y = new double[N];
		}
	}
	
	/**
	 * Wrapper to generate a context that calculates the best sampling ratio.
	 */
	protected ClusteringContext getClusteringContext(Context context) throws IOException, InterruptedException {
		return new PilotContext(context);
	}
	
	/**
	 * Get the maximum sampling ratio to get an error.
	 * Parts of this are already done by the superclass.
	 * Note that because of the minimization, this operation is slow (~5ms)
	 * @param cluster The value containing the data to evaluate.
	 * @param targetError The maximum percentage error allowed.
	 * @return [n, M/m]
	 */
	protected int[] getMaxSamplingRatio(float targetError) {
		// Calculate average cluster size (M) and variance (s^2)
		double Mavg = 0.0;
		double Mmax = 0.0;
		double s2avg = 0.0;
		// Get the variation parameters for the maximum value
		int j = 0;
		double[] yhat = new double[n];
		double var2 = 0.0;
		// Check the available clusters
		for (int i=0; i<N; i++) {
			// Check if this is a received cluster
			if (M[i] >= 0){
				// Calculate average and maximum (this could actually be done once)
				Mavg += M[i];
				if (M[i] > Mmax) {
					Mmax = M[i];
				}
				// No point on checking this for clusters with no points
				if (m[i] > 0 && y[i] > 0) {
					// Current value
					double yi = y[i];
					// Estimate the results for each cluster
					yhat[j] = auxm1[i] * yi;
					// Estimate variation
					if (s2[i] == -1) {
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
					} else if (s2[i] >= 0) {
						var2 += auxm3[i] * s2[i];
						// Calculate average value
						s2avg += s2[i];
					} else if (s2[i] == -3) {
						var2 += auxm3[i] * (0.1*yi);
						// Calculate average value
						s2avg += 0.1*yi;
					}
					// We don't reset y[i] in this case
				}
				j++;
			}
		}
		Mavg = Mavg/n;
		s2avg = s2avg/n;
		
		// Estimate the total number
		double tauhat = Nn * MultistageSamplingHelper.sum(yhat); // N/n
		
		// We use the optimization approach. If we are a pilot, we assume in the next one we will start from scratch
		//int[] result = minExecutionTime(n, tauhat, MultistageSamplingHelper.var(yhat), var2, (int) Math.round(Mavg), s2avg, targetError);
		int[] result = minExecutionTime(0, tauhat, MultistageSamplingHelper.var(yhat), var2, (int) Math.round(Mavg), s2avg, targetError);
		int bestT = result[0];
		int bestN = result[1];
		int bestR = result[2]; // Maximum sampling ratio
		
		return new int[] {bestT, bestN, bestR};
	}
	
	/**
	 * Avoid the cleaning from the superclass.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// We don't do anything here, no null keys to treat
	}
}
