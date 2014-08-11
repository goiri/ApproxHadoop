/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.hadoop.mapreduce.wikipedia;

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

import org.apache.hadoop.mapreduce.approx.multistage.ParameterPartitioner;
import org.apache.hadoop.mapreduce.approx.multistage.MultistageSamplingPartitioner;

/**
 * Count the number of links each page gets. It supports 3-stage sampling.
 */
public class WikiPageRank {
	/**
	 * Launch wikipedia page rank.
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
		options.addOption("e", "error",    true,  "Maximum error (%)");
		
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
			// Sampling ratio
			if (cmdline.hasOption("s")) {
				int samplingRate = Integer.parseInt(cmdline.getOptionValue("s"));
				conf.setInt("mapred.input.approximate.skip", samplingRate);
			}
			// Target goal
			if (cmdline.hasOption("e")) {
				int targetError = Integer.parseInt(cmdline.getOptionValue("e"));
				conf.setBoolean("mapred.input.approximate.skip.adaptive", true); // We allow dynamic sampling rate
				conf.setBoolean("mapred.map.approximate.drop.adaptive", true); // We allow dynamic sampling rate
				conf.setFloat("mapred.approximate.error.target", targetError/100f);
				conf.setBoolean("mapred.map.approximate.drop.hard", false);
			}
			
			// Create job
			Job job = new Job(conf, "Wikipedia PageRank");
			
			job.setJarByClass(WikiPageRank.class);
			
			job.setNumReduceTasks(numReducers);
			
			job.setMapperClass(WikiPageRankMapper.class);
			if (cmdline.hasOption("c")) {
				job.setReducerClass(WikiPageRankReducerIncr.class);
			} else {
				job.setReducerClass(WikiPageRankReducer.class);
			}
			
			// For this approach, we can use a combiner that sums the outputs in the maps
			job.setCombinerClass(IntSumReducer.class);
			
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
			job.setInputFormatClass(WikipediaPageInputFormat.class);
			
			// Input and output
			FileInputFormat.addInputPath(job,   new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Run and exit
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(WikiPageRank.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}
}
