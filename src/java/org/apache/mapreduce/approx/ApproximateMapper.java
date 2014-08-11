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

package org.apache.hadoop.mapreduce.approx;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Approximate Hadoop
 */
public class ApproximateMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private boolean approximate = false;

  /**
   * Approximate Hadoop.
   */
  @SuppressWarnings("unchecked")
  protected void mapApproximate(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
    super.map(key, value, context);
  }
  
  /**
   * Approximate Hadoop.
   * Define if we can approximate this map.
   */
  public void setApproximate(boolean approximate) {
    this.approximate = approximate;
  }
  
  /**
   * Approximate Hadoop.
   */
  public boolean isApproximate() {
    return this.approximate;
  }
  
  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
      if (isApproximate()) {
        mapApproximate(context.getCurrentKey(), context.getCurrentValue(), context);
      } else {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    }
    cleanup(context);
  }
}