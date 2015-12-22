package com.nextmining.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TextPair A Writable implementation that stores a pair of Text objects.
 * 
 * @author Younggue Bae
 *
 */
public class TextPairWritable implements WritableComparable<TextPairWritable> {

	private Text first;
	private Text second;
	private String delimiter;
	private String prefix;
	private String suffix;

	public TextPairWritable() {
		set(new Text(), new Text(), "\t", null, null);
	}

	public TextPairWritable(String first, String second) {
		set(new Text(first), new Text(second), "\t", null, null);
	}
	
	public TextPairWritable(String first, String second, String delimiter, String prefix, String suffix) {
		set(new Text(first), new Text(second), delimiter, prefix, suffix);
	}

	public TextPairWritable(Text first, Text second) {
		set(first, second, "\t", null, null);
	}
	
	public TextPairWritable(Text first, Text second, String delimiter, String prefix, String suffix) {
		set(first, second, delimiter, prefix, suffix);
	}

	public void set(Text first, Text second, String delimiter, String prefix, String suffix) {
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

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPairWritable) {
			TextPairWritable tp = (TextPairWritable) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return prefix + first + delimiter + second + suffix;
	}

	@Override
	public int compareTo(TextPairWritable tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}

	/** A RawComparator for comparing TextPair byte representations */
	public static class Comparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(TextPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(TextPairWritable.class, new Comparator());
	}

	/** 
	 * TextPairFirstComparator A custom RawComparator for comparing 
	 * the first field of TextPair byte representations.
	 */
	public static class FirstComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public FirstComparator() {
			super(TextPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPairWritable && b instanceof TextPairWritable) {
				return ((TextPairWritable) a).first.compareTo(((TextPairWritable) b).first);
			}
			return super.compare(a, b);
		}
	}
}
