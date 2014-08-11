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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Random;

/**
 * Approximate Hadoop: Reads lines skipping some.
 */
public class ApproximateLineRecordReader extends LineRecordReader {
  private long lineNumber = 0;
  private long skipLines = 0;
  private long prevSkipLines = 0;
  private long maxLines = -1;

  private Random rnd;
  
  public ApproximateLineRecordReader(Configuration job, FileSplit split) throws IOException {
    super(job, split);
    // Configure how many lines to approximate
    this.skipLines = job.getInt("mapred.input.approximate.skip", 1);
    if (this.skipLines < 1) {
      this.skipLines = 1;
    }
    rnd = new Random();
    // A maximum set of lines
    this.maxLines = job.getInt("mapred.input.approximate.max", -1);
  }

  public ApproximateLineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
    super(in, offset, endOffset, job);
    // Configure how many lines to approximate
    this.skipLines = job.getInt("mapred.input.approximate.skip", 1);
    if (this.skipLines < 1) {
      this.skipLines = 1;
    }
    rnd = new Random();
    // A maximum set of lines
    this.maxLines = job.getInt("mapred.input.approximate.max", -1);
  }
  
  /** 
   * Read a line skipping a few.
   */
  public synchronized boolean next(LongWritable key, Text value) throws IOException {
    // Select a random number to skip
    int randomNumber = rnd.nextInt((int)this.skipLines);
    
    // We always read one extra line, which lies outside the upper split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
      // We just read the first few lines
      if (maxLines > 0 && lineNumber > maxLines) {
         return false;
      }
    
      key.set(pos);

      int newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
      if (newSize == 0) {
        return false;
      }
      
      // We read one every X lines
      lineNumber++;
      //if ((lineNumber-1)%this.skipLines != 0) {
      //  continue;
      //}
      
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
        // We now account for the lines we need to skip the next time
        prevSkipLines = skipLines + prevSkipLines-1;
        return true;
      }
    }
    return false;
  }
}
