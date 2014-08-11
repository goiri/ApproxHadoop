/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.approx.lib.input;

import java.io.IOException;

import java.util.Random;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.SamplingRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Counters.Counter;

import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Extension to {@code LineRecordReader} that reads lines using random sampling.
 * @author Inigo Goiri
 */
public class ApproximateLineRecordReader extends LineRecordReader implements SamplingRecordReader {
  private static final Log LOG = LogFactory.getLog(ApproximateLineRecordReader.class);
  
  // Variables for input sampling
  private long lineNumber = 0; // Counter of the number of lines.
  private int skipLines = 1; // Sampling ratio
  private int maxLines = -1;
  private int prevSkipLines = 0; // How many lines we still have to skip

  // Random number generator for random sampling
  private Random rnd;
  
  /**
   * Get the sampling ratio.
   */
  public int getSamplingRatio() {
    return skipLines;
  }
  
  public long getPopulation() {
    return lineNumber;
  }
    
  /**
   * Class for sorting according to n first and then Mm in the reverse order.
   */
  class NumRatio implements Comparable {
    int t;
    int n;
    int Mm; // M/m
    long tauhat;
    int red;
    
    public NumRatio(int t, int n, int Mm, long tauhat, int red) {
      this.t = t;
      this.n = n;
      this.Mm = Mm;
      this.tauhat = tauhat;
      this.red = red;
    }
    
    public int compareTo(Object oo) {
      NumRatio o = (NumRatio) oo;
      if (o.tauhat == this.tauhat) {
        return o.Mm - this.Mm;
      }
      if (o.tauhat > this.tauhat) {
        return 1;
      } else {
        return -1;
      }
      /*if (t == o.t) {
        if (n == o.n) {
          return o.Mm - Mm;
        }
        return n - o.n;
      }
      return t - o.t;*/
      //return o.Mm - Mm;
    }
    
    public String toString() {
      return red+": tauhat="+tauhat+" t="+t+" n="+n+" M/m="+Mm;
    }
  }
    
  /**
   * This is basically an extension of {@link org.apache.hadoop.mapreduce.LineRecordReader} that add sampling.
   */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    super.initialize(genericSplit, context);
    
    // Random number generator to generate a random sample
    rnd = new Random();
    
    // Get the parameters from the user
    Configuration conf = context.getConfiguration();
    
    // Sampling ratio
    skipLines = conf.getInt("mapred.input.approximate.skip", 1);
    
    // Use adaptive sampling ratio
    if (conf.getBoolean("mapred.input.approximate.skip.adaptive", false)) {
      try {
        // How many reducers we require
        int redRequired = conf.getInt("mapred.approximate.adaptive.numreq", -1);
        // Connect to the JobTracker to get the sampling ratio required by the reducers
        // We use a hybrid between the old and the new APIs, in the newest one we should "Cluster()"
        JobClient client = new JobClient(new JobConf(conf));
        RunningJob parentJob = client.getJob(context.getJobID().toString());
        
        // Check the values for every reducer
        int numReducers = conf.getInt("mapred.reduce.tasks", 1);
        LinkedList<NumRatio> samplingRatios = new LinkedList<NumRatio>();
        org.apache.hadoop.mapred.Counters counters = parentJob.getCounters();
        // Get the required sampling ratio for each reducer
        for (int reducer=0; reducer<numReducers; reducer++) {
          org.apache.hadoop.mapred.Counters.Counter counterH  = counters.findCounter("Multistage Sampling", "Tauhat "+Integer.toString(reducer));
          org.apache.hadoop.mapred.Counters.Counter counterT  = counters.findCounter("Multistage Sampling", "Time "+Integer.toString(reducer));
          org.apache.hadoop.mapred.Counters.Counter counterN  = counters.findCounter("Multistage Sampling", "Clusters "+Integer.toString(reducer));
          org.apache.hadoop.mapred.Counters.Counter counterMm = counters.findCounter("Multistage Sampling", "SamplingRatio "+Integer.toString(reducer));
          int samplingRatio = skipLines;
          int numCluster = Integer.MAX_VALUE;
          int time = Integer.MAX_VALUE;
          long tauhat = 0;
          if (counterMm.getValue() > 0) {
            samplingRatio = (int) counterMm.getValue();
          }
          if (counterN.getValue() > 0) {
            numCluster = (int) counterN.getValue();
          }
          if (counterT.getValue() > 0) {
            time = (int) counterT.getValue();
          }
          if (counterH.getValue() > 0) {
            tauhat = counterH.getValue();
          }
          // Check which is the minimum sampling ratio we should follow
          samplingRatios.add(new NumRatio(time, numCluster, samplingRatio, tauhat, reducer));
          // Output
          LOG.info(reducer+": n="+numCluster+" M/m=" + samplingRatio+" tauhat="+tauhat);
        }
        
        
        // Select the sampling ratio according to the user requirements
        Collections.sort(samplingRatios);
        if (redRequired>0 && redRequired <= samplingRatios.size()) {
          skipLines = samplingRatios.get(redRequired-1).Mm;
          LOG.info("We set the sampling ratio to M/m=" + skipLines+" required by reducer.");
        } else {
           // By default we assume the worst case: the minimum sampling ratio
          skipLines = samplingRatios.getLast().Mm;
        }
          
      } catch (Exception e) {
        System.err.println("Error getting the sampling ratio: " + e);
      }
    }
    
    // Negative ratios don't mean anything
    if (skipLines <= 1) {
      skipLines = 1;
    } else {
      context.setStatus("Sampling "+skipLines);
    }
    
    // Maximum number of lines
    maxLines = conf.getInt("mapred.input.approximate.max", -1);
  }
  
  /**
   * An extension that skips lines from the log.
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    // Select a random number to skip
    int randomNumber = rnd.nextInt(this.skipLines);
    
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
      // Read a maximum number of lines
      if (lineNumber > 0 && lineNumber < maxLines) {
        break;
      }
    
      newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
      if (newSize == 0) {
        break;
      }
      
      // We skip one every "skipLines" lines
      lineNumber++;
      
      // We first skip the number of lines from the previous run
      if (prevSkipLines > 0) {
        prevSkipLines--;
        continue;
      }
      
      // Now we skip the selected random
      if (randomNumber > 0) {
        randomNumber--;
        prevSkipLines--;
        continue;
      }
      
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }
    }
    
    // We now account for the lines we need to skip the next time
    prevSkipLines = skipLines + prevSkipLines-1;
    
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }
}
