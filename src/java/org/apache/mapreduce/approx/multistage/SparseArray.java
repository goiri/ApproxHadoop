package org.apache.hadoop.mapreduce.approx.multistage;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

import java.io.File;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;


/**
 * Array supporting sparse values (lots of 0/null)
 */
public class SparseArray<E> implements Serializable {
	/**
	* p -> v
	*/
	public class SparseNode<E> implements Serializable {
		public int p;
		public E v;
		
		public SparseNode(int p, E v) {
			this.p = p;
			this.v = v;
		}
	}
	
	// Size of the array.
	public int size;

	// Compressed: p->v, p->v
	public LinkedList<SparseNode<E>> sparse;
	
	// Expanded
	public ArrayList<E> array; // we keep it null until expanding
	
	public SparseArray(int size) {
		this.size = size;
		// We start sparse
		this.sparse = new LinkedList<SparseNode<E>>();
	}
	
	/**
	 * Get element p from the array.
	 */
	public E get(int p) {
		if (p >= size) {
			throw new IndexOutOfBoundsException();
		}
		// Sparse
		if (array == null) {
			// Linear search because this is supposed to be small
			for (int i=0; i<sparse.size(); i++) {
				if (sparse.get(i).p == p) {
					return sparse.get(i).v;
				}
			}
			return null;
		// Complete
		} else {
			return array.get(p);
		}
	}
	
	/**
	 * Set element p in the array.
	 */
	public void set(int p, E v) {
		if (p >= size) {
			throw new IndexOutOfBoundsException();
		}
		this.checkMaxSize();
		// Sparse array
		if (array == null) {
			// Change it
			boolean found = false;
			for (int i=0; i<this.sparse.size(); i++) {
				if (this.sparse.get(i).p == p) {
					this.sparse.get(i).v = v;
					found = true;
					break;
				}
			}
			// Add it
			if (!found) {
				this.sparse.addLast(new SparseNode<E>(p, v));
			}
		// Complete array
		} else {
			array.set(p, v);
		}
	}
	
	/**
	 * Check if the array is actually sparse.
	 */
	public boolean isSparse() {
		return array == null;
	}
	
	/**
	 * If we have too many elements in the array, we make it complete.
	 */
	private void checkMaxSize() {
		if (array==null && sparse.size() > 0.1*size) { // 10% of size
			// Create array
			array = new ArrayList<E>(size);
			for (int i=0; i<size; i++) {
				array.add(null);
			}
			// Dump sparse values into the array
			while (sparse.size() > 0) {
				SparseNode<E> node = sparse.pop();
				// Add value
				//array[pos] = val;
				array.set(node.p, node.v);
			}
			// Clear the sparse method
			sparse = null;
		}
	}
	
	/**
	 * Get an array with the values
	 */
	public E[] values(int n) {
		ArrayList<E> ret = new ArrayList<E>();
		for (int i=0; i<size && i<n; i++) {
			E v = get(i);
			ret.add(v);
		}
		return (E[]) ret.toArray();
	}
	
	/**
	 * String version of the array.
	 */
	public String toString() {
		String ret = "[";
		if (this.get(0)==null) {
			ret += "0";
		} else {
			ret += this.get(0);
		}
		for (int i=1; i<size; i++) {
			if (this.get(i)==null) {
				ret += ",0";
			} else {
				ret += "," + this.get(i);
			}
		}
		ret += "]";
		return ret;
	}
	
	/**
	 * Tester.
	 */
	public static void main(String[] args) {
		System.out.println("Tester");
		SparseArray<Long> array = new SparseArray<Long>(60);
		array.set(5, 11L);
		array.set(10, 12L);
		System.out.println(array.isSparse());
		array.set(15, 13L);
		System.out.println(array.isSparse());
		array.set(40, 14L);
		System.out.println(array.isSparse());
		array.set(20, 15L);
		array.set(30, 16L);
		System.out.println(array.isSparse());
		array.set(25, 17L);
		array.set(15, 18L);
		System.out.println(array.isSparse());
		array.set(50, 19L);
		array.set(32, 21L);
		//array.set(60, 21L);
		array.set(0, 11L);
		System.out.println(array);
		System.out.println(array.isSparse());
		
		
		try {
			/*FileOutputStream fout = new FileOutputStream("/tmp/mola");
			ObjectOutputStream oos = new ObjectOutputStream(fout); 
			oos.writeObject(array);
			fout.close();*/
			
			// Read
			System.out.println("check file.......");
			File file = new File("/scratch/hadoop-goiri/mapred/local/taskTracker/goiri/jobcache/job_201405141909_0103/attempt_201405141909_0103_r_000000_0/work/tmp/approx1170333968943084158.tmp");
			System.out.println(file.length());
			FileInputStream fin = new FileInputStream(file);
			
			// Read
			ObjectInputStream iis = new ObjectInputStream(fin); 
			Object o = iis.readObject();
			//System.out.println(o);
			
			System.out.println(o.getClass());
			
			File.createTempFile("tmpfile", "tmp");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
