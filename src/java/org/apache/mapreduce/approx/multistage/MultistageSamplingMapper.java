package org.apache.hadoop.mapreduce.approx.multistage;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.approx.lib.input.ApproximateLineRecordReader;
import org.apache.hadoop.mapreduce.approx.SamplingRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.log4j.Logger;

/**
 * Mapper that allows sending parameters to all the reducers for multistage sampling.
 */
public abstract class MultistageSamplingMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT extends WritableComparable> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger(MultistageSamplingMapper.class);

	// We keep track of this for multistage sampling
	protected long m  =  0; // Sample size
	protected long t  = -1; // Terciary cluster size. We don't use this by default
	protected long s2 = -1; // Variance. We don't use this by default
	
	private int tmap = 0;
	
	/**
	 * This is a wrapper for Context that gets keys and adds an ID at the end to identify the cluster the data comes from.
	 * Incremental doesn't require this only the default reducer.
	 */
	public class ClusteringContext extends Context {
		Context context;
		int sendTaskId = 0;
		boolean precise = false;
		
		public ClusteringContext(Context context) throws IOException, InterruptedException {
			// This is just a wrapper, so we don't create anything
			super(context.getConfiguration(), context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, null);

			// Save the context
			this.context = context;
			this.sendTaskId = context.getTaskAttemptID().getTaskID().getId();
			this.precise = context.getConfiguration().getBoolean("mapred.job.precise", false);
		}
		
		/**
		 * Overwrite of regular write() to capture values and do clustering if needed. If we run precise, pass it to the actual context.
		 */
		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,InterruptedException {
			if (!this.precise && key instanceof Text) {
				// Sort method with just one more character at the end
				byte[] byteId = new byte[] {(byte) (sendTaskId/128), (byte) (sendTaskId%128)};
				context.write((KEYOUT) new Text(key.toString()+new String(byteId)), value);
				// Long method that is human readable
				//context.write((KEYOUT) new Text(key.toString()+String.format("-%05d", sendTaskId)), value);
			} else {
				context.write(key, value);
			}
		}

		// We overwrite the following methods to avoid problems, ideally we would forward everything
		@Override
		public float getProgress() {
			return context.getProgress();
		}
		
		@Override
		public void progress() {
			context.progress();
		}
		
		@Override
		public void setStatus(String status) {
			context.setStatus(status);
		}
		
		@Override
		public Counter getCounter(Enum<?> counterName) {
			return context.getCounter(counterName);
		}
		
		@Override
		public Counter getCounter(String groupName, String counterName) {
			return context.getCounter(groupName, counterName);
		}
	}
	
	/**
	 * We use this to keep track of the fields and send it to the reducers, the user can decide to use something else or add others.
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		
		long t0 = System.currentTimeMillis();
		
		// Create the context that adds an id for clustering (just if requried)
		Context newcontext = context;
		// If we don't do incremental, we have to IDs to the keys
		Configuration conf = context.getConfiguration();
		
		if (!conf.getBoolean("mapred.job.precise", false) && !conf.getBoolean("mapred.tasks.incremental.reduction", false)) {
			newcontext = new ClusteringContext(context);
		}
		
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), newcontext);
			m++;
		}
		
		// Calculate how long this took
		tmap = (int) (System.currentTimeMillis()-t0)/1000;
		
		cleanup(context);
	}
	
	/**
	 * Cleanup function that reports how many fields have been processed.
	 * This is the default case where each process item is an element m
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Get the the sampling ratio from the reader
		long M = 1;
		RecordReader<KEYIN,VALUEIN> reader = context.getReader();
		if (reader instanceof SamplingRecordReader) {
			M = ((SamplingRecordReader)reader).getPopulation();
		}
		
		if (m>0) {
			LOG.info("Cluster characteristics: m="+m+" M="+M+" M/m="+(M/m)+" t="+t+" s2="+s2);
		} else {
			LOG.info("Cluster characteristics: m="+m+" M="+M+" M/m=inf t="+t+" s2="+s2);
		}
		
		// We send the statistically relevant information to everybody if we are sampling
		Configuration conf = context.getConfiguration();
		if (!conf.getBoolean("mapred.job.precise", false)) {
			// Integer format
			if (IntWritable.class.equals(context.getMapOutputValueClass())) {
				if (m >= 0) {
					sendValue(context, MultistageSamplingReducer.m_SAMPLED, (int) m);
				}
				if (t >= 0) {
					sendValue(context, MultistageSamplingReducer.T_SAMPLED, (int) t);
				}
				if (s2 >= 0 || s2 == -3) {
					sendValue(context, MultistageSamplingReducer.S_SAMPLED, (int) s2);
				}
				sendValue(context, MultistageSamplingReducer.M_SAMPLED, (int) M);
				sendValue(context, MultistageSamplingReducer.t_SAMPLED, (int) tmap);
				// Send the cluster marks
				sendValue(context, MultistageSamplingReducer.CLUSTERINI, 0);
				sendValue(context, MultistageSamplingReducer.CLUSTERFIN, 0);
			// Long format
			} else {
				if (m >= 0) {
					sendValue(context, MultistageSamplingReducer.m_SAMPLED, m);
				}
				if (t >= 0) {
					sendValue(context, MultistageSamplingReducer.T_SAMPLED, t);
				}
				if (s2 >= 0 || s2 == -3) {
					sendValue(context, MultistageSamplingReducer.S_SAMPLED, s2);
				}
				sendValue(context, MultistageSamplingReducer.M_SAMPLED, (long) M);
				sendValue(context, MultistageSamplingReducer.t_SAMPLED, (long) tmap);
				// Send the cluster marks
				sendValue(context, MultistageSamplingReducer.CLUSTERINI, 0L);
				sendValue(context, MultistageSamplingReducer.CLUSTERFIN, 0L);
			}
		}
	}
	
	/**
	 * Set the secondary (ssu) cluster size.
	 */
	protected void setM(long m) {
		this.m = m;
	}
	
	/**
	 * Set the terciary (tsu) cluster size.
	 */
	protected void setT(long t) {
		this.t = t;
	}
	
	/**
	 * Set the variance for this cluster.
	 */
	protected void setS(long s2) {
		this.s2 = s2;
	}
	
	/**
	 * Send a parameter (inside of the data) to all reducers.
	 */
	protected void sendValue(Context context, String param, Long value) throws IOException, InterruptedException {
		int numReducers = context.getConfiguration().getInt("mapred.reduce.tasks", 1);
		for (int r=0; r<numReducers; r++) {
			context.write((KEYOUT) new Text(String.format(param, context.getTaskAttemptID().getTaskID().getId(), r)), (VALUEOUT) new LongWritable(value));
		}
	}
	
	protected void sendValue(Context context, String param, Integer value) throws IOException, InterruptedException {
		int numReducers = context.getConfiguration().getInt("mapred.reduce.tasks", 1);
		for (int r=0; r<numReducers; r++) {
			context.write((KEYOUT) new Text(String.format(param, context.getTaskAttemptID().getTaskID().getId(), r)), (VALUEOUT) new IntWritable(value));
		}
	}
}
