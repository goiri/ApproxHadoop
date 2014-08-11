package org.apache.hadoop.mapreduce.apache;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingMapper;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducer;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingReducerIncr;
import org.apache.hadoop.mapreduce.approx.multistage.ParameterPartitioner;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingPartitioner;
import org.apache.hadoop.mapreduce.approx.multistage.SparseArray;
import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;

import org.apache.hadoop.mapreduce.approx.lib.input.ApproximateTextInputFormat;


/**
 * Approximate log analysis count.
 * hack
 * host
 * dateweek
 * size
 * totalsize
 * pagesize
 * page
 * browser
 */
public class ApacheLogAnalysis {
	//public static final String NUMLINES_SAMPLED = "$NUMLINES-%d$";
	//public static final String NUMLINES_SAMPLED_MATCH = "\\$NUMLINES-(\\d)+\\$";
	
	/**
	* Mapper that processes apache log lines.
	*/
	public static class ApacheLogMapper extends MultistageSamplingMapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		private String task = null;
		
		// A couple tasks need this variable
		private long totalSize = 0;
		
		// Auxilliary variables to compute standard deviation on the fly
		private int    num   = 0; // Counter on how many elements
		private double avg   = 0.0; // Average
		private double stdev = 0.0;  // Standard deviation
		
		private SimpleDateFormat indateformat  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		//private SimpleDateFormat outdateformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		//private SimpleDateFormat outdateformat = new SimpleDateFormat("yyyy/MM/dd HH"); // Day/hour
		//private SimpleDateFormat outdateformat = new SimpleDateFormat("HH"); // Hour
		//private SimpleDateFormat outdateformat = new SimpleDateFormat("EEE"); // Day of the week
		//private SimpleDateFormat outdateformat = new SimpleDateFormat("EEE HH"); // Day of the week
		
		public void setup(Context context) {
			task = context.getConfiguration().get("task");
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				// "Inspiration" from: http://www.java2s.com/Code/Java/Regular-Expressions/ParseanApachelogfilewithStringTokenizer.htm
				StringTokenizer matcher = new StringTokenizer(value.toString());
				String hostname = matcher.nextToken();
				matcher.nextToken(); // eat the "-"
				matcher.nextToken("["); // again
				String datetime = matcher.nextToken("]").substring(1);
				matcher.nextToken("\"");
				String request = matcher.nextToken("\"");
				matcher.nextToken(" "); // again
				String response = matcher.nextToken();
				String byteCount = matcher.nextToken();
				matcher.nextToken("\"");
				String referer = matcher.nextToken("\"");
				matcher.nextToken("\""); // again
				String userAgent = "-";
				try {
					 userAgent = matcher.nextToken("\"");
				} catch(Exception e) {}
				
				if (task != null) {
					// Check who is trying to hack us
					if (task.equalsIgnoreCase("hack")) {
						// "POST /cgi-bin/php-cgi?XXX HTTP/1.1"
						String[] requestSplit = request.split(" ");
						if (requestSplit.length > 1) {
							String address = requestSplit[1];
							String[] keywords = { "/w00tw00t", "/phpMyAdmin", "/pma", "/myadmin", "/MyAdmin", "/phpTest", "/cgi-bin/php", "/cgi-bin/php5", "/cgi-bin/php-cgi" };
							boolean found = false;
							for (String keyword : keywords) {
								if (address.startsWith(keyword)) {
									found = true;
									break;
								}
							}
							// We note that that user is pushing us
							if (found) {
								word.set(hostname);
								context.write(word, one);
							}
						}
					// Hosts visiting the web
					} else if (task.equalsIgnoreCase("host")) {
						word.set(hostname);
						context.write(word, one);
					// Analyze the access time of the visits
					// 24/Nov/2013:06:25:45 -0500 -> Date
					} else if (task.equalsIgnoreCase("dateweek")) {
						Date date = indateformat.parse(datetime);
						SimpleDateFormat outdateformat = new SimpleDateFormat("EEE HH"); // Day of the week
						word.set(outdateformat.format(date));
						context.write(word, one);
					// Size per object
					} else if (task.equalsIgnoreCase("size")) {
						long bytes = (Long.parseLong(byteCount)/100)*100; // Round for histogram
						word.set(Long.toString(bytes));
						context.write(word, one);
					// Total size per object
					} else if (task.equalsIgnoreCase("totalsize")) {
						word.set("Total");
						long bytes = Long.parseLong(byteCount);
						context.write(word, new LongWritable(bytes));
						// To send afterwards
						//totalSize += bytes;
						addValueforStdev(1.0*bytes);
					// Page traffic
					} else if (task.equalsIgnoreCase("pagesize")) {
						int lastIndex = request.indexOf("?");
						if (lastIndex > 0) {
							String aux = request.substring(0, lastIndex);
							word.set(aux);
						} else {
							word.set(request);
						}
						long bytes = Long.parseLong(byteCount);
						context.write(word, new LongWritable(bytes));
						// To send afterwards
						//addValueforStdev(1.0*bytes);
					// Page visit
					} else if (task.equalsIgnoreCase("page")) {
						int lastIndex = request.indexOf("?");
						if (lastIndex > 0) {
							String aux = request.substring(0, lastIndex);
							word.set(aux);
						} else {
							word.set(request);
						}
						context.write(word, one);
					// Browser
					} else if (task.equalsIgnoreCase("browser")) {
						word.set(userAgent);
						context.write(word, one);
					// Default
					} else {
						System.err.println("Unknown option:" + task);
					}
				}
			} catch (Exception e) {
				System.err.println("Error processing line for task \""+task+"\": " + value.toString());
				System.err.println(e);
				e.printStackTrace();
			}
		}
		
		/**
		 * A couple of tasks need to sum everything up.
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {
			// We modify "m", "si2" and "t" parameters
			if (task.equalsIgnoreCase("totalsize")) {
				setS((long) getVariance());
			} else if (task.equalsIgnoreCase("pagesize")) {
				// Mark to use worst case for variation
				setS(-3);
			}
			// Make the super class take care of sending the rest
			super.cleanup(context);
		}
	
		/**
		*A method to calculate the mean and standard deviation of a series of numbers.
		* http://www.buluschek.com/?p=140
		* http://www.cs.berkeley.edu/~mhoemmen/cs194/Tutorials/variance.pdf
		*/
		private void addValueforStdev(double xk) {
			num++;
			double d = xk-avg; // is actually xk - Mk-1, as Mk was not yet updated
			stdev += (num-1)*d*d/num;
			avg += d/num;
		}
		
		private double getStdev() {
			return Math.sqrt(stdev/num);
		}
		
		// si2
		private double getVariance() {
			return stdev/num;
		}
	}
	
	/**
	 * Reducer that takes into account the approximation factor.
	 */
	public static class ApacheLogAnalysisReducer extends MultistageSamplingReducer<Text,LongWritable,Text,LongWritable> {
		private LongWritable result = new LongWritable();
		
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
	
	public static class ApacheLogAnalysisReducerIncr extends MultistageSamplingReducerIncr<Text,LongWritable,Text,LongWritable> {
		private LongWritable result = new LongWritable();
		
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
	
	/**
	 * Launch apache log analysis.
	 */
	public static void main(String[] args) throws Exception {
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
		
		try {
			CommandLine cmdline = new GnuParser().parse(options, otherArgs);
			// Input output
			String input  = cmdline.getOptionValue("i");
			String output = cmdline.getOptionValue("o");
			if (input == null || output == null) {
				throw new ParseException("No input/output option");
			}
			// Task to perform
			String optionsStr = "";
			if (cmdline.hasOption("t")) {
				conf.set("task", cmdline.getOptionValue("t"));
				optionsStr+=" "+cmdline.getOptionValue("t");
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
			if (!optionsStr.equals("")) {
				optionsStr = " ["+optionsStr.trim()+"]";
			}
			
			// Create job
			Job job = new Job(conf, "Apache log "+optionsStr);
			
			job.setJarByClass(ApacheLogAnalysis.class);
			
			job.setNumReduceTasks(numReducers);
			
			job.setMapperClass(ApacheLogMapper.class);
			if (cmdline.hasOption("c")) {
				job.setReducerClass(ApacheLogAnalysisReducerIncr.class);
			} else {
				job.setReducerClass(ApacheLogAnalysisReducer.class);
			}
			
			// For this approach, we can use a combiner that sums the outputs in the maps
			job.setCombinerClass(LongSumReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			// We need a partitioner that sends clustering information to all reducers
			if (!cmdline.hasOption("p")) {
				if (cmdline.hasOption("c")) {
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
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(ApacheLogAnalysis.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}
}
