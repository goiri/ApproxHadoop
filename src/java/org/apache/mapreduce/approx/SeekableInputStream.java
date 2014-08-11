/**
 * Copyright 2011 Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.approx;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.compress.*;

public class SeekableInputStream extends FilterInputStream implements Seekable {
  private final Seekable seek;
  private final SplitCompressionInputStream sin;
  public SeekableInputStream(FSDataInputStream in) {
    super(in);
    this.seek = in;
    this.sin = null;
  }
  public SeekableInputStream(SplitCompressionInputStream cin) {
    super(cin);
    this.seek = cin;
    this.sin = cin;
  }
  public SeekableInputStream(CompressionInputStream cin, FSDataInputStream in) {
    super(cin);
    this.seek = in;
    this.sin = null;
  }
  public static SeekableInputStream getInstance(Path path, long start, long end, FileSystem fs, CompressionCodecFactory compressionCodecs) throws IOException {
    CompressionCodec codec = compressionCodecs.getCodec(path);
    FSDataInputStream din = fs.open(path);
    if (codec != null) {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        SplittableCompressionCodec scodec = (SplittableCompressionCodec)codec;
        SplitCompressionInputStream cin = scodec.createInputStream
          (din, decompressor, start, end,
           SplittableCompressionCodec.READ_MODE.BYBLOCK);
        return new SeekableInputStream(cin);
      } else {
        // non-splittable compression input stream
        // no seeking or offsetting is needed
        assert start == 0;
        CompressionInputStream cin = codec.createInputStream(din, decompressor);
        return new SeekableInputStream(cin, din);
      }
    } else {
      // non compression input stream
      // we seek to the start of the split
      din.seek(start);
      return new SeekableInputStream(din);
    }
  }
  public static SeekableInputStream getInstance(FileSplit split, FileSystem fs, CompressionCodecFactory compressionCodecs) throws IOException {
    return getInstance(split.getPath(), split.getStart(), split.getStart() + split.getLength(), fs, compressionCodecs);
  }
  public SplitCompressionInputStream getSplitCompressionInputStream() { return this.sin; }
  public long getPos() throws IOException { return this.seek.getPos(); }
  public void seek(long pos) throws IOException { this.seek.seek(pos); } 
  public boolean seekToNewSource(long targetPos) throws IOException { return this.seek.seekToNewSource(targetPos); }
  @Override public String toString() {
    return this.in.toString();
  }
}