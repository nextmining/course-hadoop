package com.nextmining.hadoop.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TextPair A Writable implementation that stores a pair of Text and Double objects.
 * 
 * @author Younggue Bae
 *
 */
public class TextDoublePairWritable implements WritableComparable<TextDoublePairWritable> {

	private String first;
	private double second;
	private String delimiter;
	private String prefix;
	private String suffix;

	public TextDoublePairWritable() {
		set("", 0.0, "\t", null, null);
	}

	public TextDoublePairWritable(String first, double second) {
		set(first, second, "\t", null, null);
	}
	
	public TextDoublePairWritable(String first, double second, String delimiter, String prefix, String suffix) {
		set(first, second, delimiter, prefix, suffix);
	}

	public void set(String first, double second, String delimiter, String prefix, String suffix) {
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

	public String getFirst() {
		return first;
	}

	public double getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeDouble(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readDouble();
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + new Double(second).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextDoublePairWritable) {
			TextDoublePairWritable tdp = (TextDoublePairWritable) o;
			return first.equals(tdp.first) && new Double(second).equals(new Double(tdp.second));
		}
		return false;
	}

	@Override
	public String toString() {
		return prefix + first + delimiter + second + suffix;
	}

	@Override
	public int compareTo(TextDoublePairWritable tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return new Double(second).compareTo(new Double(tp.second));
	}

  /**
   * Convenience method for comparing two ints.
   */
  public static int compare(double a, double b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
}
