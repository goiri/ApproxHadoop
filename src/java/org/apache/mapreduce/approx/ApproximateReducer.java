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

import org.apache.hadoop.mapreduce.Reducer;

/** 
 * Approximate Hadoop.
 */
public class ApproximateReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  private boolean approximate = false;
  
  /**
   * Approximate reducer.
   */
  @SuppressWarnings("unchecked")
  protected void reduceApproximate(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
    super.reduce(key, values, context);
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
   * Use approximation if it is approximate.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKey()) {
      if (isApproximate()) {
        reduceApproximate(context.getCurrentKey(), context.getValues(), context);
      } else {
        reduce(context.getCurrentKey(), context.getValues(), context);
      }
    }
    cleanup(context);
  }
}
