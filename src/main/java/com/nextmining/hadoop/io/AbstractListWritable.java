package com.nextmining.hadoop.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Writable for ArrayList containing instances of a class.
 *
 * @param <E> Element element
 */
public abstract class AbstractListWritable<E extends Writable> extends ArrayList<E>
	implements Writable, Configurable {
	
  /** Defining a layout version for a serializable class. */
  private static final long serialVersionUID = 1L;
  /** Used for instantiation */
  private Class<E> refClass = null;
  
  /** Configuration */
  private Configuration conf;
  
  private Map<Integer, String> separators = new HashMap<Integer, String>();

  /**
   * Using the default constructor requires that the user implement
   * setClass(), guaranteed to be invoked prior to instantiation in
   * readFields()
   */
  public AbstractListWritable() { }
  
  /**
   * Constructor with another {@link AbstractListWritable}.
   *
   * @param arrayListWritable Array list to be used internally.
   */
  public AbstractListWritable(AbstractListWritable<E> arrayListWritable) {
    super(arrayListWritable);
  }
  
  /**
	 * Creates an ArrayListWritable object from an ArrayList.
	 */
  public AbstractListWritable(ArrayList<E> list) {
    super(list);
  }

  /**
   * This constructor allows setting the refClass during construction.
   *
   * @param refClass internal type class
   */
  public AbstractListWritable(Class<E> refClass) {
    super();
    this.refClass = refClass;
  }

  /**
   * This is a one-time operation to set the class type
   *
   * @param refClass internal type class
   */
  public void setClass(Class<E> refClass) {
    if (this.refClass != null) {
      throw new RuntimeException(
          "setClass: refClass is already set to " +
              this.refClass.getName());
    }
    this.refClass = refClass;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  	this.clear();
  	
    int numValues = in.readInt();					// read number of values
    String strSeparators = in.readUTF();	// read separators
    separators = parseSeparators(strSeparators);	
    ensureCapacity(numValues);
    
    for (int i = 0; i < numValues; i++) {
      E element = ReflectionUtils.newInstance(refClass, conf);
      element.readFields(in);							// read a element value
      add(element);												// store it in values
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int numValues = size();
    out.writeInt(numValues);							// write number of values
    out.writeUTF(separators.toString());	// write separators
    for (int i = 0; i < numValues; i++) {
      get(i).write(out);
    }
  }
  
  public void addSeparator(String separator) {
  	int currentIndex = this.size() - 1;
  	if (currentIndex >= 0) {
  		
  		separators.put(currentIndex, separator);
  	}
  }
  
  private Map<Integer, String> parseSeparators(String strSeparators) {
  	Map<Integer, String> mapSeparators = new HashMap<Integer, String>();
  	
  	strSeparators = strSeparators.substring(1, strSeparators.length()-1);
  	
  	if (!strSeparators.equals("")) {
	  	String[] arrSep = strSeparators.split(",");
	  	
	  	for (String keyValPair : arrSep) {
	  		String[] keyVal = keyValPair.split("=");
	  		mapSeparators.put(Integer.parseInt(keyVal[0].trim()), keyVal[1]);
	  	}
  	}

  	return mapSeparators;
  }
  
  @Override
  public String toString() {
  	if (separators.size() == 0) {
  		return super.toString();
  	}
  	else {
  		StringBuilder sb = new StringBuilder();
  		
  		int sepSize = 0;
  		int prevPos = 0;
  		for (Map.Entry<Integer, String> entry : separators.entrySet()) {
  			sepSize++;
  			int sepPos = entry.getKey();
  			String separator = entry.getValue();
 
  			if (sepPos < size() - 1) {
	  			List<E> sublist = subList(prevPos, sepPos+1);
	  			String strSublist = sublist.toString();
	  			if (sublist.size() <= 1) {
	  				strSublist = strSublist.substring(1, strSublist.length()-1);
	  			}
	  			sb.append(strSublist).append(separator);
	  			
	  			if (sepSize == separators.size()) {
	  				List<E> remainSublist = subList(sepPos+1, size());	
	  				String strRemainSublist = remainSublist.toString();
		  			if (remainSublist.size() <= 1) {
		  				strRemainSublist = strRemainSublist.substring(1, strRemainSublist.length()-1);
		  			}
		  			//sb.append(strRemainSublist).append(separator);
		  			sb.append(strRemainSublist);
	  			}
	  		
	  			prevPos = sepPos + 1;
  			}
  		}
  		
  		return sb.toString();
  	}
  }

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = conf;
  }
}
