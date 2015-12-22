package com.nextmining.hadoop.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sortable implements WritableComparable<Sortable> {
  
	public static final String ASC = "ascending";
	public static final String DESC = "descending";
	
	public static final String STRING_TYPE = "string";
	public static final String NUMERIC_TYPE = "numeric";
	
	protected String sortOption = "ascending";
	protected Object value;
	protected String datatype = "string";
	
	public Sortable() { }
	
	public Sortable(String sortOption, String datatype) {
		this.sortOption = sortOption;
		this.datatype = datatype;
	}
	
	public Sortable(String sortOption, String datatype, Object value) {
		this.sortOption = sortOption;
		this.datatype = datatype;
		this.value = value;
	}
 	
	public void setSortOption(String sortOption) {
		this.sortOption = sortOption;
	}
	
	public Object getValue() {
		return value;
	}
	
	public void setValue(Object value) {
		this.value = value;
	}
	
	public String getDatattype() {
		return datatype;
	}
	
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(datatype);
		if (datatype.equalsIgnoreCase(STRING_TYPE)) {
			out.writeUTF(new String(value.toString()));
		}
		else if (datatype.equalsIgnoreCase(NUMERIC_TYPE)) {
			out.writeDouble(new Double(value.toString()));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		datatype = in.readUTF();
		if (datatype.equalsIgnoreCase(STRING_TYPE)) {
			value = in.readUTF();
		}
		else if (datatype.equalsIgnoreCase(NUMERIC_TYPE)) {
			value = in.readDouble();
		}
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public int compareTo(Sortable o) {
		if (sortOption.equalsIgnoreCase(DESC)) {
			if (datatype.equalsIgnoreCase(STRING_TYPE)) {
				return new String(value.toString()).compareTo(new String(o.getValue().toString()));	
			}
			else if (datatype.equalsIgnoreCase(NUMERIC_TYPE)) {
				return new Double(value.toString()).compareTo(new Double(o.getValue().toString()));	
			}
		} else if (sortOption.equalsIgnoreCase(ASC)) {
			if (datatype.equalsIgnoreCase(STRING_TYPE)) {
				return new String(o.getValue().toString()).compareTo(new String(value.toString()));	
			}
			else if (datatype.equalsIgnoreCase(NUMERIC_TYPE)) {
				return new Double(o.getValue().toString()).compareTo(new Double(value.toString()));	
			}
		}

		return 0;
	}

}
