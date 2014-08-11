package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.io.IntWritable;

/**
 * This is just for outputting. The ideal would be to have value as protected in IntWritable but...
 */
public class ApproximateIntWritable extends IntWritable {
	private int value;
	private int range;

	public ApproximateIntWritable(int value, double range) {
		this.value = value;
		this.range = (int) Math.ceil(range);
	}
	
	public String toString() {
		return Integer.toString(value)+"+/-"+range;
	}
	
	/**
	 * Normalized error.
	 */
	public double getError() {
		if (this.value == 0) {
			return 0.0;
		}
		return this.range/this.value;
	}
	
	/*public ApproximateIntWritable() {}

	public ApproximateIntWritable(int value) {
		set(value);
	}
	
	public ApproximateIntWritable(int value, double range) {
		set(value);
		this.range = (int) Math.ceil(range);
	}

	public void set(int value) {
		this.value = value;
	}

	public int get() { return value; }

	public void readFields(DataInput in) throws IOException {
		value = in.readInt();
		in.readChar();
		in.readChar();
		in.readChar();
		range = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
		out.writeChars("+/-");
		out.writeInt(range);
	}

	public boolean equals(Object o) {
		if (!(o instanceof IntWritable))
			return false;
		ApproximateIntWritable other = (ApproximateIntWritable)o;
		return this.value == other.value;
	}

	public int hashCode() {
		return value;
	}

	public int compareTo(Object o) {
		int thisValue = this.value;
		int thatValue = ((ApproximateIntWritable)o).value;
		return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}

	public String toString() {
		return Integer.toString(value)+"+/-"+range;
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int thisValue = readInt(b1, s1);
			int thatValue = readInt(b2, s2);
			return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
	}
	
	static {                                        // register this comparator
		WritableComparator.define(IntWritable.class, new Comparator());
	}*/
}