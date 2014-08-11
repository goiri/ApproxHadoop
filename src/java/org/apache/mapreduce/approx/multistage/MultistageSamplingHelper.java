package org.apache.hadoop.mapreduce.approx.multistage;

import org.apache.commons.math.distribution.TDistribution;
import org.apache.commons.math.distribution.TDistributionImpl;


/**
 * Set of useful functions for multistage sampling
 */
public class MultistageSamplingHelper {
	/**
	 * Get the t-score based on the t-student distribution.
	 */
	public static double getTScore(int degrees, double confidence) {
		double tscore = 1.96; // By default we use the normal distribution
		try {
			TDistribution tdist = new TDistributionImpl(degrees);
			//double confidence = 0.95; // 95% confidence => 0.975
			tscore = tdist.inverseCumulativeProbability(1.0-((1.0-confidence)/2.0)); // 95% confidence 1-alpha
		} catch (Exception e) { }
		return tscore;
	}
	
	/**
	 * Calculate summation of a list of numbers.
	 */
	protected static double sum(int[] arr) {
		double sum = 0.0;
		for (int a : arr) {
			sum += a;
		}
		return sum;
	}
	protected static double sum(long[] arr) {
		double sum = 0.0;
		for (long a : arr) {
			sum += a;
		}
		return sum;
	}
	
	protected static double sum(double[] arr) {
		double sum = 0.0;
		for (double a : arr) {
			sum += a;
		}
		return sum;
	}
	
	/**
	* Calculate variance of a list of numbers. Note that this is the estimated variance (n-1).
	*/
	protected static double var(double[] arr) {
		// Infinite variance for a single element
		if (arr.length <= 1) {
			return Double.MAX_VALUE; // x/n-1 = x/0 = infinity
		}
		
		double avg = 0.0;
		for (double a : arr) {
			avg += a;
			//avg += a / arr.length; // It looks like we don't overflow, so let's do it outside
		}
		avg = avg/arr.length;
		
		// Calculate the variance
		double variance = 0.0;
		for (double a : arr) {
			double aux = a-avg;
			variance += (aux*aux);
			//variance += (aux*aux) / (arr.length-1.0);  // It looks like we don't overflow, so let's do it outside
		}
		variance = variance/(arr.length-1);
		
		return variance;
	}
	
	/**
	 * Convert a format string into a match string.
	 */
	public static String formatToMatch(String format) {
		String ret = new String(format);
		ret = ret.replaceAll("%d", "(\\\\d)+");
		ret = ret.replaceAll("\\$", "\\\\\\$");
		return ret;
	}
	
	/**
	 * Creates a fancy string for a number
	 */
	public static String toNumberString(double number) {
		String suffix = "";
		if (number > 1000000) {
			number = number/1000000.0;
			suffix = "M";
		} else if (number > 1000) {
			number = number/1000.0;
			suffix = "K";
		}
		return String.format("%.1f%s", number, suffix);
	}
}