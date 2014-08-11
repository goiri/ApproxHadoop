package org.apache.hadoop.mapreduce.wikipedia;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.approx.multistage.ParameterPartitioner;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingPartitioner;

import org.apache.hadoop.mapreduce.approx.XMLInputFormat;

/**
 * This job produces the histogram of the lenghts of wikipedia articles.
 */
public class WikiLengths {
	/**
	 * Launch wikipedia length histogram.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
		// Parsing options
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Options for the rest
		Options options = new Options();
		options.addOption("i", "input",    true,  "Input file");
		options.addOption("o", "output",   true,  "Output file");
		options.addOption("r", "reduces",  true,  "Number of reducers");
		options.addOption("c", "incr",     false, "Incremental reducers");
		options.addOption("d", "drop",     true,  "Percentage of maps to drop");
		options.addOption("n", "inidrop",  true,  "Percentage of maps to drop initially");
		options.addOption("p", "precise",  false, "Precise job");
		options.addOption("s", "sampling", true,  "Sampling rate 1/X");
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
			if (cmdline.hasOption("t")) {
				conf.set("task", cmdline.getOptionValue("t"));
			}
			// Reduces
			int numReducers = 1;
			if (cmdline.hasOption("r")) {
				numReducers = Integer.parseInt(cmdline.getOptionValue("r"));
			}
			// Incremental reducers
			if (cmdline.hasOption("c")) {
				conf.setBoolean("mapred.tasks.incremental.reduction", true);
				conf.setBoolean("mapred.tasks.clustering", true); // We arrange the intermediate keys by clusters
			}
			// Dropping maps
			if (cmdline.hasOption("d")) {
				float dropPercentage = Float.parseFloat(cmdline.getOptionValue("d"));
				conf.setInt("mapred.map.approximate.drop.extratime", 1);
				conf.setFloat("mapred.map.approximate.drop.percentage", dropPercentage/100);
			}
			// Dropping maps
			if (cmdline.hasOption("n")) {
				float dropPercentage = Float.parseFloat(cmdline.getOptionValue("n"));
				conf.setFloat("mapred.map.approximate.drop.ini.percentage", 1-(dropPercentage/100)); // Percentage of maps we initially drop
			}
			// Precise job
			if (cmdline.hasOption("p")) {
				conf.setBoolean("mapred.job.precise", true);
				conf.setBoolean("mapred.tasks.incremental.reduction", false);
			}
			if (cmdline.hasOption("s")) {
				int samplingRate = Integer.parseInt(cmdline.getOptionValue("s"));
				conf.setInt("mapred.input.approximate.skip", samplingRate);
			}
			
			// Create job
			Job job = new Job(conf, "Wikipedia lengths histogram");
			
			job.setJarByClass(WikiLengths.class);
			
			job.setNumReduceTasks(numReducers);
			
			job.setMapperClass(WikiLengthsMapper.class);
			if (cmdline.hasOption("c")) {
				job.setReducerClass(WikiLengthsReducerIncr.class);
			} else {
				job.setReducerClass(WikiLengthsReducer.class);
			}
			
			// For this approach, we can use a combiner that sums the outputs in the maps
			job.setCombinerClass(IntSumReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// We need a partitioner that sends clustering information to all reducers
			if (!cmdline.hasOption("p")) {
				if (cmdline.hasOption("c")) {
					job.setPartitionerClass(ParameterPartitioner.class);
				} else {
					job.setPartitionerClass(MultistageSamplingPartitioner.class);
				}
			}
			
			// Approximate Hadoop
			//job.setInputFormatClass(WikipediaPageInputFormat.class);
			job.setInputFormatClass(XMLInputFormat.class);
			
			// Input and output
			FileInputFormat.addInputPath(job,   new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Run and exit
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(WikiLengths.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}

	/*public static void runJob(Configuration conf, String input, String output) throws Exception {
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		// These two lines enable bzip output from the reducer
		//conf.setBoolean("mapred.output.compress", true);
		//conf.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
		// SimpleDateFormat ymdhms = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss";
		SimpleDateFormat ymdhms = new SimpleDateFormat("HH:mm:ss");
		Job job = new Job(conf, "wikPageLengths " + ymdhms.format(new Date()));
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setJarByClass(WikiLengths.class);
		
		job.setMapperClass(WikiLengthsMapper .class);
		job.setReducerClass(WikiLengthsReducer.class);
		
		job.setInputFormatClass(XMLInputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wikilengths <in> <out>");
			System.exit(2);
		}
		runJob(conf, otherArgs[0], otherArgs[1]);
	}*/
}