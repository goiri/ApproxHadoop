package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import java.util.Date;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingMapper;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducer;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducerIncr;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingPilot;
import org.apache.hadoop.mapreduce.approx.multistage.SparseArray;
import org.apache.hadoop.mapreduce.approx.multistage.ParameterPartitioner;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingPartitioner;
import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.mapreduce.approx.lib.input.ApproximateTextInputFormat;

/**
 * Check the popularity of wikipedia pages.
 * pages
 * project
 */
public class WikiPopularity {
	/**
	 * Mapper that processes wikpedia log lines.
	 */
	public static class WikiPopularityMapper extends MultistageSamplingMapper<LongWritable,Text,Text,LongWritable> {
		//LongWritable result = new LongWritable();
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		/**
		 * Gets a log line from wikipedia parses it and produces an output.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				/*
				StringTokenizer itr = new StringTokenizer(value.toString());
				
				// Parse line
				String date    = itr.nextToken();
				String hour    = itr.nextToken();
				
				String project = itr.nextToken(); // language
				String page    = itr.nextToken();
				
				String size    = itr.nextToken();
				*/
				
				String line = value.toString().replaceAll("\t", " ");
				String[] lineSplit = line.split(" ");
				if (lineSplit.length >= 4) {
					String date    = lineSplit[0];
					String hour    = lineSplit[1];
					
					String project = lineSplit[2]; // language
					String page    = lineSplit[3];
					
					if (lineSplit.length >= 5) {
						String size = lineSplit[4];
					}
					
					// Perform actual task
					String task = context.getConfiguration().get("task");
					if ("project".equalsIgnoreCase(task)) {
						word.set(project);
						context.write(word, one);
					} else {
						word.set(project+" "+page);
						context.write(word, one);
					}
				} else {
					System.out.println("Cannot split: " + value.toString());
				}
			} catch (Exception e) {
				System.out.println("Error processing line: " + value.toString());
				System.out.println(e);
			}
		}
	}
	
	/**
	 * Reducer that takes into account the approximation factor.
	 */
	public static class WikiPopularityReducer extends MultistageSamplingReducer<Text,LongWritable,Text,LongWritable> {
		LongWritable result = new LongWritable();
		/**
		 * Reduce function that uses the default collection approach.
		 */
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class WikiPopularityReducerIncr extends MultistageSamplingReducerIncr<Text,LongWritable,Text,LongWritable> {
		LongWritable result = new LongWritable();
		/**
		 * Reduce function that uses the default collection approach.
		 */
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

		/*public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			// Calculate aggregated value
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			
			// Check for the number of pages in the current cluster
			if (key.toString().matches(WikiPopularity.NUMLINES_SAMPLED_MATCH)) {
				clusterLines[curCluster] = sum;
			// Add to the proper cluster
			} else {
				synchronized(this) {
					// We have to create the cluster structure for new keys
					if (!clusters.containsKey(key.toString())) {
						//clusters.put(key.toString(), new long[numCluster]);
						clusters.put(key.toString(), new SparseArray<Long>(numCluster));
					}
					//clusters.get(key.toString())[curCluster] = sum;
					clusters.get(key.toString()).set(curCluster, sum);
				}
			}
			
			// Debugging memory problem
			/ *if (clusters.size() > prevsize+(10*1000)) {
				System.out.println("heapMaxSize  = " + Runtime.getRuntime().maxMemory()/(1024*1024) + " MB");
				System.out.println("heapSize     = " + Runtime.getRuntime().totalMemory()/(1024*1024) + " MB");
				System.out.println("heapFreeSize = " + Runtime.getRuntime().freeMemory()/(1024*1024) + " MB");
				int auxS = 0;
				for (SparseArray<Long> array : clusters.values()) {
					if (array.isSparse()) {
						auxS++;
					}
				}
				System.out.println("Clusters: " + auxS + "/" + clusters.size());
				
				try {
					File tmpFile = File.createTempFile("approx", ".tmp");
					FileOutputStream fout = new FileOutputStream(tmpFile);
					ObjectOutputStream oos = new ObjectOutputStream(fout); 
					oos.writeObject(clusters);
					oos.close();
					fout.close();
					
					System.out.format("%s: File: %s %.2fMB\n", new Date(), tmpFile, tmpFile.length()/(1024.0*1024.0));
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				// We create a new map
				clusters.clear();
				
				prevsize = clusters.size();
			}* /
		}*/
		
		/*protected synchronized void checkQuality(Context context, boolean output) throws IOException, InterruptedException {
			long t0 = System.currentTimeMillis();
		
			double maxAbs = 0.0;
			double maxError = 0.0;
			double maxErrorRel = 0.0;
		
			// Get current paramters
			int numMaps = context.getConfiguration().getInt("mapred.map.tasks", -1);
			int skipPages = context.getConfiguration().getInt("mapred.input.approximate.skip", 1);
			
			// Multistage sampling
			int N = numMaps;
			int n = curCluster+1;
			
			// Calculations populatons in each cluster
			long[] mi = new long[n];
			double[] auxmi1 = new double[n];
			double[] auxmi2 = new double[n];
			double[] auxmi3 = new double[n];
			for (int i=0; i<n; i++) {
				mi[i] = clusterLines[i];
				double Mi = mi[i]*skipPages;
				auxmi1[i] = 1.0*Mi/mi[i];
				auxmi2[i] = 1.0*Mi*(Mi-mi[i])/(mi[i]-1.0);
			}
			
			// Get the t-score for the current distribution
			double tscore = MultistageSamplingReducer.getTScore(n-1, 0.95);
			
			// Go over all the keys
			for (Object key : clusters.keySet()) {
				// Calculate total estimation
				double[] yhati = new double[n];
				double var2 = 0.0;
				double tauhat = 0.0;
				for (int i=0; i<n; i++) {
					//double yi = clusters.get(key)[i];
					double yi = 0.0;
					try {
						yi = clusters.get(key).get(i);
					} catch (Exception e) { }
					yhati[i] = auxmi1[i] * yi;
					tauhat += (1.0*N/n) * yhati[i];
					double pi = 1.0 * yi / mi[i];
					var2 += auxmi2[i] * pi * (1.0-pi);
				}
				// Calculate total variance
				Double setauhat = Math.sqrt(1.0*(N*(N-n)*var(yhati))/n + (1.0*N/n)*var2);
				if (setauhat.isNaN()) { 
					setauhat = Double.MAX_VALUE;
				}
				
				// Calculate the maximum relative error
				if (tscore*setauhat > maxError) {
					maxAbs = tauhat;
					maxError = tscore*setauhat;
					maxErrorRel = 100.0*tscore*setauhat/tauhat;
				}
				
				// Output the estimation
				if (output) {
					Text outkey = new Text((String)key);
					ApproximateLongWritable outval = new ApproximateLongWritable((long) tauhat, tscore*setauhat);
					context.write(outkey, outval);
				}
			}
			System.out.format("%s: %.1fs %d/%d max error %.1fM+/-%.1fM (+/-%.2f%%) Keys=%d\n", new Date(), (System.currentTimeMillis()-t0)/1000.0, n, N, maxAbs/1000000, maxError/1000000, maxErrorRel, clusters.size());
		}*/
	}
	
	/**
	 * Launch wikipedia log analysis.
	 */
	public static void main(String[] args) throws Exception {
		// Get Hadoop options first
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Options for the rest
		Options options = new Options();
		options.addOption("i", "input",    true,  "Input file");
		options.addOption("o", "output",   true,  "Output file");
		options.addOption("r", "reduces",  true,  "Number of reducers");
		options.addOption("p", "precise",  false, "Precise job");
		options.addOption("d", "drop",     true,  "Percentage of maps to drop");
		options.addOption("n", "inidrop",  true,  "Percentage of maps to drop initially");
		options.addOption("h", "harddrop", false, "Drop already running jobs");
		options.addOption("s", "sampling", true,  "Sampling rate 1/X");
		options.addOption("c", "incr",     false, "Incremental reducers");
		options.addOption("t", "task",     true,  "Task: project, other");
		options.addOption("e", "error",    true,  "Maximum error (%)");
		options.addOption("a", "adaptive", false, "Run an adaptive job");
		options.addOption(     "pilot",    false, "Execute pilot sample");
		
		try {
			CommandLine cmdline = new GnuParser().parse(options, otherArgs);
			// Input output
			String input  = cmdline.getOptionValue("i");
			String output = cmdline.getOptionValue("o");
			if (input == null || output == null) {
				throw new ParseException("No input/output option");
			}
			// Task to perform
			if (cmdline.hasOption("t")) {
				conf.set("task", cmdline.getOptionValue("t"));
			}
			// Reduces
			int numReducers = 1;
			if (cmdline.hasOption("r")) {
				numReducers = Integer.parseInt(cmdline.getOptionValue("r"));
			}
			// Precise job
			if (cmdline.hasOption("p")) {
				conf.setBoolean("mapred.job.precise", true);
				conf.setBoolean("mapred.tasks.incremental.reduction", false);
				// Check not possible cases
				if (cmdline.hasOption("c")) {
					System.err.println("Incremental reducing (-c) is not available with precise execution");
					System.exit(1);
				}
				if (cmdline.hasOption("d") || cmdline.hasOption("n")) {
					System.err.println("Dropping (-d or -n)is not available with precise execution");
					System.exit(1);
				}
				if (cmdline.hasOption("s")) {
					System.err.println("Input sampling (-s) is not available with precise execution");
					System.exit(1);
				}
				if (cmdline.hasOption("e")) {
					System.err.println("Error target (-e) is not available with precise execution");
					System.exit(1);
				}
				if (cmdline.hasOption("a")) {
					System.err.println("Adaptive sampling (-a) is not available with precise execution");
					System.exit(1);
				}
				if (cmdline.hasOption("pilot")) {
					System.err.println("Pilot sample (-pilot) is not available with precise execution");
					System.exit(1);
				}
				
			}
			// Sampling ratio
			if (cmdline.hasOption("s")) {
				int samplingRate = Integer.parseInt(cmdline.getOptionValue("s"));
				conf.setInt("mapred.input.approximate.skip", samplingRate);
			}
			// Dropping maps
			if (cmdline.hasOption("d")) {
				float dropPercentage = Float.parseFloat(cmdline.getOptionValue("d"));
				conf.setInt("mapred.map.approximate.drop.extratime", 1);
				conf.setFloat("mapred.map.approximate.drop.percentage", dropPercentage/100);
			}
			// Dropping initial maps
			if (cmdline.hasOption("n")) {
				float dropPercentage = Float.parseFloat(cmdline.getOptionValue("n"));
				conf.setFloat("mapred.map.approximate.drop.ini.percentage", 1-(dropPercentage/100)); // Percentage of maps we initially drop
			}
			// Drop maps that are already running
			if (cmdline.hasOption("h")) {
				conf.setBoolean("mapred.map.approximate.drop.hard", true);
			}
			// Incremental reducers
			if (cmdline.hasOption("c")) {
				conf.setBoolean("mapred.tasks.incremental.reduction", true);
				conf.setBoolean("mapred.tasks.clustering", true); // We arrange the intermediate keys by clusters
			}
			// Target error goal
			if (cmdline.hasOption("e")) {
				float targetError = Float.parseFloat(cmdline.getOptionValue("e"));
				
				// We only check N cases (top sampling ratio and reduces for dropping)
				conf.setFloat("mapred.approximate.error.target", targetError/100f);
				conf.setInt("mapred.approximate.adaptive.numreq", 1);
			}
			// Pilot is only the first wave
			if (cmdline.hasOption("pilot")) {
				// Just run the first wave to achieve the pilot
				conf.setFloat("mapred.map.approximate.drop.percentage", 0.01f/100f); // 0.1%
				//conf.setFloat("mapred.map.approximate.drop.ini.percentage", 1f-(80f/2486));
				if (!cmdline.hasOption("s")) {
					// A pilot is just an estimation
					conf.setInt("mapred.input.approximate.skip", 100);
				}
			}
			// Run a pilot before the actual job
			if (cmdline.hasOption("pilot") && cmdline.hasOption("a")) {
				// We have to run two jobs: pilot and sample. Let's go with the pilot
				Job job = new Job(conf, "Wikipedia access pilot");
				job.setJarByClass(WikiPopularity.class);
				job.setNumReduceTasks(numReducers);
				job.setMapperClass(WikiPopularityMapper.class);
				job.setReducerClass(MultistageSamplingPilot.class);
				job.setCombinerClass(LongSumReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(LongWritable.class);
				job.setPartitionerClass(MultistageSamplingPartitioner.class);
				job.setInputFormatClass(ApproximateTextInputFormat.class);
				
				// Input and output
				FileInputFormat.addInputPath(job,   new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output+"-pilot"));
				
				// Run the job
				System.out.println("Running pilot sample first...");
				job.waitForCompletion(true);
				
				// Collect pilot sample results
				long maxSamplingRate = 1;
				long numMaps = Long.MAX_VALUE;
				long tauhat = 0;
				Counters counters = job.getCounters();
				for (int reducer=0; reducer<numReducers; reducer++) {
					Counter counterT  = counters.findCounter("Multistage Sampling", "Time "+Integer.toString(reducer));
					Counter counterN  = counters.findCounter("Multistage Sampling", "Clusters "+Integer.toString(reducer));
					Counter counterH  = counters.findCounter("Multistage Sampling", "Tauhat "+Integer.toString(reducer));
					Counter counterMm = counters.findCounter("Multistage Sampling", "SamplingRatio "+Integer.toString(reducer));
					//System.out.println(reducer+": t="+counterT.getValue()+" n="+counterN.getValue()+"\tM/m=" + counterMn.getValue());
					System.out.println(reducer+": n="+counterN.getValue()+" M/m=" + counterMm.getValue());
					// Pick the minimum number of clusters (that's usually the fastest, the user could pick)
					/*if (counterN.getValue() < numMaps) {
						numMaps         = counterN.getValue();
						maxSamplingRate = counterMm.getValue();
					}*/
					/*if (counterMm.getValue() > maxSamplingRate) {
						//maxMaps = counterN.getValue();
						maxSamplingRate = counterMm.getValue();
					}*/
					if (counterH.getValue() > tauhat) {
						tauhat = counterH.getValue();
						numMaps         = counterN.getValue();
						maxSamplingRate = counterMm.getValue();
					}
				}
				System.out.println("Full job expected to finish in "+numMaps+" maps with M/m="+maxSamplingRate+"... Run it!");
				
				// Set the sampling rate for the next job
				conf.setInt("mapred.input.approximate.skip", (int) maxSamplingRate); // Set sampling rate
				
				// Check if it's worth approximating
				/*if (numMaps == job.getConfiguration().getInt("mapred.map.tasks", 1) && maxSamplingRate == 1) {
					System.out.println("We require too much precission: no point on approximating.");
				}*/
			}
			// Set for adaptive sampling (both drop and input sampling)
			if (cmdline.hasOption("a")) {
				conf.setBoolean("mapred.tasks.incremental.reduction", true);
				conf.setBoolean("mapred.tasks.clustering", true); 
				// Dynamic adaptation
				conf.setBoolean("mapred.input.approximate.skip.adaptive", true);
				conf.setBoolean("mapred.map.approximate.drop.adaptive",   true);
				// We are adaptive, we don't drop randomly
				conf.setFloat("mapred.map.approximate.drop.percentage", 1f); // 100f
				conf.setFloat("mapred.map.approximate.drop.ini.percentage", 0f); // 100f
			}
			
			
			// Create job
			Job job = new Job(conf, "Wikipedia access analysis");
			
			job.setJarByClass(WikiPopularity.class);
			
			job.setNumReduceTasks(numReducers);
			
			job.setMapperClass(WikiPopularityMapper.class);
			if (cmdline.hasOption("c") || cmdline.hasOption("a")) {
				job.setReducerClass(WikiPopularityReducerIncr.class);
			} else if (cmdline.hasOption("pilot")) {
				job.setReducerClass(MultistageSamplingPilot.class);
			} else {
				job.setReducerClass(WikiPopularityReducer.class);
			}
			
			// For this approach, we can use a combiner that sums the outputs in the maps
			job.setCombinerClass(LongSumReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			// We need a partitioner that sends clustering information to all reducers
			if (!cmdline.hasOption("p")) {
				if (cmdline.hasOption("c") || cmdline.hasOption("a")) {
					job.setPartitionerClass(ParameterPartitioner.class);
				} else {
					job.setPartitionerClass(MultistageSamplingPartitioner.class);
				}
			}
			
			// Approximate Hadoop
			job.setInputFormatClass(ApproximateTextInputFormat.class);
			
			// Input and output
			FileInputFormat.addInputPath(job,   new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Run and exit
			boolean success = job.waitForCompletion(true);
			
			// Report pilot results
			if (cmdline.hasOption("pilot") && !cmdline.hasOption("a")) {
				Counters counters = job.getCounters();
				for (int reducer=0; reducer<numReducers; reducer++) {
					Counter counterN  = counters.findCounter("Multistage Sampling", "Clusters "+Integer.toString(reducer));
					Counter counterMm = counters.findCounter("Multistage Sampling", "SamplingRatio "+Integer.toString(reducer));
					System.out.println(reducer+": n="+counterN.getValue()+" M/m=" + counterMm.getValue());
				}
			}
			
			System.exit(success ? 0 : 1);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(WikiPopularity.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}
}
