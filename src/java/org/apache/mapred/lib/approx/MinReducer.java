package org.apache.hadoop.mapred.lib.approx;

import java.io.IOException;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This reducer reads values from the maps and selects the minimum.
 * It uses characteristics from the map output distribution to provide a minimum with a given error requirement.
 */
public class MinReducer implements Reducer<Text,Text,Text,Text> {
	private static final Log LOG = LogFactory.getLog(MinReducer.class);

	//private IntWritable result = new IntWritable();
	private double minKey = 999999.9;
	private String minValue = null;
	private OutputCollector<Text, Text> output = null;
	
	// We store the outputs of the maps (original distribution sample)
	private List<Double> sample = null;
	
	// Error requirements
	private float reqConfidence; // Between 0 and 1
	private float reqError;      // Between 0 and 100%
	
	/**
	 * Selects the confidence/error requirements.
	 */
	public void configure(JobConf conf) {
		this.sample = new LinkedList<Double>();
		
		// Read configuration values for the minimum error required
		this.reqConfidence = conf.getFloat("approx.minconfidence", 0.95f); // Between 0 and 1
		this.reqError      = conf.getFloat("approx.minerror", 0.0f);       // In percentage
	}
	
	/**
	 * Collects the outputs from the maps and selects the one with the minimum cost.
	 */
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		this.output = output;
		if (key.toString().equals("Result")) {
			while (values.hasNext()) {
				// Save intermediate result
				Text val = values.next();
				output.collect(key, val);
				
				// Extract minimum cost
				double cost = getCostFromValue(val);
				if (cost < minKey) {
					minKey = cost;
					minValue = val.toString();
					
				}
				// Add to the sample
				this.sample.add(cost);
				
				// Calculate the minimum margin 
				double minError = this.getError();
				
				// Calculate the minimum with margin
				//double minValueWithError  = this.sample.get(0)*(1.0-(minError/100.0));
				
				LOG.info(String.format("Current error with a confidence of %.1f is %.2f%% (<= %.2f%%?)", this.reqConfidence*100.0, minError, this.reqError));
				
				if (this.reqError > 0 && minError <= this.reqError) { // For example 5% error
					// We mark that we can start dropping
					LOG.info("Minimum error requirement achieved! We can drop the other maps!");
					// We set the status to dropping and the JobTracker will take care
					reporter.setStatus("dropping");
				}
			}
		}
	}
	
	/**
	 * Get the error when estimating the minimum based on the current sample.
	 */
	private double getError() {
		// Average for the current sample
		int n = 0;
		double avg = 0.0;
		for (double v : this.sample) {
			avg += v;
			n++;
		}
		avg = avg/n;
		
		// Standard deviation for the current sample
		double stdev = 0.0;
		for (double v : this.sample) {
			stdev += Math.pow(v-avg, 2);
		}
		stdev = Math.sqrt(stdev/(n-1));
		
		// Calculate the error for the standard deviation
		double maxError = (getZScore(this.reqConfidence)*stdev)/Math.sqrt(n); // Based on CLT
		
		// Calculate the standard deviation taking into account the potential sampling error
		double stdevCorrected = 0.0;
		for (double v : this.sample) {
			// We add the maximum error for the estimation
			stdevCorrected += Math.pow(Math.abs(v-avg)+maxError, 2);
		}
		stdevCorrected = Math.sqrt(stdevCorrected/(n-1));
		
		// Calculate minimum error end return it
		return getMinDistError(this.reqConfidence, stdevCorrected, n);
	}
	
	
	/**
	 * Get the error to get a minimum with a given confidence after n samples.
	 * @param confidence Confidence between 0 and 1. 0.95 would be 95% confidence.
	 * @param stdev Sample standard deviation (S)
	 * @param n Size of the sample (n)
	 * @return Maximum estimating error for the defined distribution.
	 */
	private double getMinDistError(double confidence, double stdev, int n) {
		// The regular way would be to use this formula but it gets into imaginary numbers and other weirdnesses
		// double minerror = Math.sqrt((120*Math.pow(stdev, 3))*(Math.log(-1.0/(0.95-1))/Math.log(n)-1));
	
		// We use dicotomic search to calculate the minimum error we can guarrantee
		double ini = 0.0;
		double fin = 100.0;
		while (fin-ini > 0.01) {
			double mid = (ini+fin)/2.0;
			double confMid = confidenceMinimum(mid, stdev, n);
			if (confMid > confidence) {
				fin = mid;
			} else {
				ini = mid;
			}
		}
		return fin;
		
		/*
		// Linear search
		double minerrorTest = 0.0;
		for (double error = 0.1; error < 100.0; error += 0.1) {
			if (confidenceMinimum(error, stdevCorrected, n) > 0.95) {
				minerrorTest = error;
				break;
			}
		}
		*/
	}
	
	/**
	 * Calculate the confidence of the minimum for a distribution with a standard deviation and a number of samples.
	 * @param error Error (interval) in percentage (er)
	 * @param stdev Sample standard deviation (S)
	 * @param n Size of the sample (n)
	 * @return Confidence of the minimum (a value from 0 to 1 which represents the percentile, eg, 0.95 is 95% confidence)
	 */
	private double confidenceMinimum(double error, double stdev, int n) {
		// We obtain this formula from experiments with multimple minimum distrigutions based on GEV
		//                               1
		// confidence =  1 - --------------------------
		//                    /         1          \ ^n
		//                   |  1 + --------- er^2  |
		//                    \     120 x S^2      /
		return 1.0 - 1.0/Math.pow(1 + (1/(120.0*Math.pow(stdev, 3)))*Math.pow(error, 2), n);
	}
	
	/**
	 * Parse an output line from the DC placement tool and extract the cost.
	 */
	private double getCostFromValue(Text value) {
		String result = value.toString();
		result = result.split("f=")[1];
		result = result.substring(0, result.indexOf("M"));
		return Double.parseDouble(result);
	}
	
	/**
	 * Get the Z-score for a normal distribution.
	 */
	private static double getZScore(double confidence) {
		if (confidence == 0)          { return 0.0; } 
		else if (confidence <= 0.05)  { return 0.0627; } 
		else if (confidence <= 0.10)  { return 0.1257; } 
		else if (confidence <= 0.15)  { return 0.1891; } 
		else if (confidence <= 0.20)  { return 0.2533; } 
		else if (confidence <= 0.25)  { return 0.3186; } 
		else if (confidence <= 0.30)  { return 0.3853; } 
		else if (confidence <= 0.40)  { return 0.5244; } 
		else if (confidence <= 0.50)  { return 0.67; } 
		else if (confidence <= 0.60)  { return 0.84; } 
		else if (confidence <= 0.70)  { return 1.04; } 
		else if (confidence <= 0.75)  { return 1.15; } 
		else if (confidence <= 0.80)  { return 1.28; } 
		else if (confidence <= 0.85)  { return 1.44; }
		else if (confidence <= 0.90)  { return 1.645; } 
		else if (confidence <= 0.95)  { return 1.96; }
		else if (confidence <= 0.98)  { return 2.33; } 
		else if (confidence <= 0.99)  { return 2.575; }
		else if (confidence <= 0.995) { return 2.81; } 
		else if (confidence <= 0.999) { return 3.09; }
		return 5.0;
	}
	
	/**
	 * We write the final result. We use the estimation of the error and a confidence interval.
	 */
	public void close() {
		try {
			LOG.info("Final sample: " + this.sample);
			
			// Calculate the minimum with the confidence margin
			double minCost = Collections.min(this.sample);
			double minCostWithError = minCost * (1.0 - (this.getError()/100.0));
			
			// Save the result into the output
			if (minValue == null) {
				LOG.error("No samples");
			} else {
				String aux = String.format("(%.2f-%.2f]: ", minCostWithError, minCost)  + " "+ minValue;
				output.collect(new Text("Final result"), new Text(aux));
				LOG.info(aux);
			}
		} catch (Exception e) {
			System.out.println("Failure: " + e);
		}
	}
}
