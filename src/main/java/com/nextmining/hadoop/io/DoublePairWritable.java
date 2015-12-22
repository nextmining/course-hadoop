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
public class DoublePairWritable implements WritableComparable<DoublePairWritable> {

	public static final String ASC = "ascending";
	public static final String DESC = "descending";
	private String sortOption = "ascending";
	private int sortColumn;
	private double first;
	private double second;
	private String delimiter;
	private String prefix;
	private String suffix;

	public DoublePairWritable() {
		set(ASC, 0, 0.0, 0.0, ", ", "(", ")");
	}
	
	public DoublePairWritable(double first, double second) {
		set(ASC, 0, first, second, ", ", "(", ")");
	}
	
	public DoublePairWritable(double first, double second, String delimiter, String prefix, String suffix) {
		set(ASC, 0, first, second, delimiter, prefix, suffix);
	}

	public DoublePairWritable(String sortOption, int sortColumn, double first, double second) {
		set(sortOption, sortColumn, first, second, ", ", "(", ")");
	}
	
	public void set(double first, double second) {
		set(ASC, 0, first, second, ", ", "(", ")");
	}

	public void set(String sortOption, int sortColumn, double first, double second, String delimiter, String prefix, String suffix) {
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

	public double getFirst() {
		return first;
	}
	
	public void setFirst(double first) {
		this.first = first;
	}

	public double getSecond() {
		return second;
	}
	
	public void setSecond(double second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(first);
		out.writeDouble(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readDouble();
		second = in.readDouble();
	}

	@Override
	public String toString() {
		return prefix + first + delimiter + second + suffix;
	}

	@Override
	public int compareTo(DoublePairWritable o) {

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
}