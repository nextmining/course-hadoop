package com.nextmining.hadoop.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link WritableComparable} which encapsulates an ordered pair of signed integers.
 * 
 * @author Younggue Bae
 */
public class IntPairWritable implements WritableComparable<IntPairWritable> {

	public static final String ASC = "ascending";
	public static final String DESC = "descending";
	private String sortOption = "ascending";
	private int sortColumn;
	private int first;
	private int second;
	private String delimiter;
	private String prefix;
	private String suffix;

	public IntPairWritable() {
		set(ASC, 0, 0, 0, ", ", "(", ")");
	}

	public IntPairWritable(String sortOption, int sortColumn, int first, int second) {
		set(sortOption, sortColumn, first, second, ", ", "(", ")");
	}
	
	public IntPairWritable(int first, int second) {
		set(ASC, 0, first, second, ", ", "(", ")");
	}
	
	public IntPairWritable(int first, int second, String delimiter, String prefix, String suffix) {
		set(ASC, 0, first, second, delimiter, prefix, suffix);
	}

	public void set(int first, int second) {
		set(ASC, 0, first, second, ", ", "(", ")");
	}
	
	public void set(String sortOption, int sortColumn, int first, int second, String delimiter, String prefix, String suffix) {
		this.sortOption = sortOption;
		this.sortColumn = sortColumn;
		this.first = first;
		this.second = second;
		this.delimiter = delimiter;
		if (prefix == null) {
			prefix = "";
		}
		if (suffix == null) {
			suffix = "";
		}
		this.prefix = prefix;
		this.suffix = suffix;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}
	
	public int getSecond() {
		return second;
	}
	
	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public String toString() {
		return prefix + first + delimiter + second + suffix;
	}

	@Override
	public int compareTo(IntPairWritable o) {

		if (sortColumn == 0) {
			if (sortOption.equalsIgnoreCase(DESC)) {
				return new Double(first).compareTo(new Double(o.getFirst()));
			} else if (sortOption.equalsIgnoreCase(ASC)) {
				return new Double(o.getFirst()).compareTo(new Double(first));
			}
		} else if (sortColumn == 1) {
			if (sortOption.equalsIgnoreCase(DESC)) {
				return new Double(second).compareTo(new Double(o.getSecond()));
			} else if (sortOption.equalsIgnoreCase(ASC)) {
				return new Double(o.getSecond()).compareTo(new Double(second));
			}
		}

		return 0;
	}

	/**
	 * Convenience method for comparing two ints.
	 */
	public static int compare(int a, int b) {
		return (a < b ? -1 : (a == b ? 0 : 1));
	}
}
