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

package org.apache.hadoop.mapreduce.approx;

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.ApproximateLineRecordReader;
import org.apache.hadoop.mapreduce.SamplingRecordReader;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Counters.Counter;

import java.util.Random;
import java.util.LinkedList;
import java.util.Collections;

/**
 * A simple {@link org.apache.hadoop.mapreduce.InputFormat} for XML documents ({@code
 * org.apache.hadoop.mapreduce} API). The class recognizes begin-of-document and end-of-document
 * tags only: everything between those delimiting tags is returned in an uninterpreted {@code Text}
 * object.
 *
 * @author Jimmy Lin
 */
public class XMLInputFormat extends TextInputFormat {
  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  /**
   * Create a record reader for a given split. The framework will call
   * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
   * the split is used.
   *
   * @param split the split to be read
   * @param context the information about the task
   * @return a new record reader
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new XMLRecordReader();
  }

  /**
   * Simple {@link org.apache.hadoop.mapreduce.RecordReader} for XML documents ({@code
   * org.apache.hadoop.mapreduce} API). Recognizes begin-of-document and end-of-document tags only:
   * everything between those delimiting tags is returned in a {@link Text} object.
   *
   * @author Jimmy Lin
   */
  public static class XMLRecordReader extends RecordReader<LongWritable, Text> implements SamplingRecordReader {
    private static final Logger LOG = Logger.getLogger(XMLRecordReader.class);

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private long pos;
    private InputStream fsin = null;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private CompressionCodec codec = null;
    private Decompressor decompressor = null;
    
    // To perform random sampling for this file
    private long numPages = 0;
    private int skipPages = 0;
    private int prevSkipPages = 0;
    private Random rnd = new Random();
    
    private long recordStartPos;

    private final LongWritable key = new LongWritable();
    private final Text value = new Text();

    /**
     * Return the sampling ratio (M/m).
     */
    public int getSamplingRatio() {
      return skipPages;
    }
    
    /**
     * Return the population (M).
     */
    public long getPopulation() {
      return numPages;
    }
    
    /**
     * Class for sorting according to t first, then n and finally Mm in the reverse order.
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
        if (o.tauhat == tauhat) {
          return o.Mm - Mm;
        }
        return (int) (o.tauhat - tauhat);
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
        return red+": t="+t+" n="+n+" M/m="+Mm;
      }
    }
    
    /**
     * Called once at initialization.
     *
     * @param input the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit input, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      if (conf.get(START_TAG_KEY) == null || conf.get(END_TAG_KEY) == null)
        throw new RuntimeException("Error! XML start and end tags unspecified!");

      startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
      endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
      
      // Initial approximation ratio
      skipPages = conf.getInt("mapred.input.approximate.skip", 1);
      
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
            int samplingRatio = skipPages;
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
            skipPages = samplingRatios.get(redRequired-1).Mm;
            LOG.info("We set the sampling ratio to M/m=" + skipPages+" required by reducer.");
          } else {
             // By default we assume the worst case: the minimum sampling ratio
            skipPages = samplingRatios.getFirst().Mm;
          }
        } catch (Exception e) {
          System.err.println("Error getting the sampling ratio: " + e);
        }
      }
      
      // Negative ratios don't mean anything
      if (skipPages < 1) {
        skipPages = 1;
      }
      
      FileSplit split = (FileSplit) input;
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      codec = compressionCodecs.getCodec(file);

      FileSystem fs = file.getFileSystem(conf);

      if (isCompressedInput()) {
        LOG.info("Reading compressed file " + file + "...");
        FSDataInputStream fileIn = fs.open(file);
        decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          // We can read blocks
          final SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
          fsin = cIn;
          start = cIn.getAdjustedStart();
          end = cIn.getAdjustedEnd();
        } else {
          // We cannot read blocks, we have to read everything
          fsin = new DataInputStream(codec.createInputStream(fileIn, decompressor));
          
          end = Long.MAX_VALUE;
        }
      } else {
        LOG.info("Reading uncompressed file " + file + "...");
        FSDataInputStream fileIn = fs.open(file);

        fileIn.seek(start);
        fsin = fileIn;

        end = start + split.getLength();
      }

      recordStartPos = start;

      // Because input streams of gzipped files are not seekable, we need to keep track of bytes
      // consumed ourselves.
      pos = start;
    }

    /**
     * Read the next key, value pair.
     *
     * @return {@code true} if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (getFilePosition() < end) {
        // We skip a few pages and remember how many we skipped before
        int rndSkipPages = rnd.nextInt(skipPages);
        int currentSkipPages = prevSkipPages + rndSkipPages;
        prevSkipPages = skipPages-rndSkipPages-1;
        for (int i=0; i<currentSkipPages; i++) {
          if(!readUntilMatch(startTag, false)) {
            return false;
          } else {
            numPages++;
          }
        }
        
        if (readUntilMatch(startTag, false)) {
          numPages++;
          recordStartPos = pos - startTag.length;

          try {
            buffer.write(startTag);
            if (readUntilMatch(endTag, true)) {
              key.set(recordStartPos);
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            // Because input streams of gzipped files are not seekable, we need to keep track of
            // bytes consumed ourselves.

            // This is a sanity check to make sure our internal computation of bytes consumed is
            // accurate. This should be removed later for efficiency once we confirm that this code
            // works correctly.

            if (fsin instanceof Seekable) {
              // The position for compressed inputs is weird
              if (!isCompressedInput()) {
                if (pos != ((Seekable) fsin).getPos()) {
                  throw new RuntimeException("bytes consumed error!");
                }
              }
            }

            buffer.reset();
          }
        }
      }
      return false;
    }

    /**
     * Returns the current key.
     *
     * @return the current key or {@code null} if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    /**
     * Returns the current value.
     *
     * @return current value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    /**
     * Closes the record reader.
     */
    @Override
    public void close() throws IOException {
      fsin.close();
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
      }
    }
    
    private boolean isCompressedInput() {
      return (codec != null);
    }
    
    protected long getFilePosition() throws IOException {
      long retVal;
      if (isCompressedInput() && null != fsin && fsin instanceof Seekable) {
        retVal = ((Seekable)fsin).getPos();
      } else {
        retVal = pos;
      }
      return retVal;
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
        throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        
        // end of file:
        if (b == -1)
          return false;
            
        // increment position (bytes consumed)
        pos++;
        
        // save to buffer:
        if (withinBlock)
          buffer.write(b);

        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length)
            return true;
        } else
          i = 0;
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && getFilePosition() >= end)
          return false;
      }
    }
  }
}
