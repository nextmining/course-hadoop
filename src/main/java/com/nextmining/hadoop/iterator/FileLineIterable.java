package com.nextmining.hadoop.iterator;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Iterable representing the lines of a text file. It can produce an {@link Iterator} over those lines. This
 * assumes the text file's lines are delimited in a manner consistent with how {@link java.io.BufferedReader}
 * defines lines.
 *
 */
public final class FileLineIterable implements Iterable<String> {

  private final InputStream is;
  private final Charset encoding;
  private final boolean skipFirstLine;
  private final String origFilename;
  
  /** Creates a  over a given file, assuming a UTF-8 encoding. */
  public FileLineIterable(File file) throws IOException {
    this(file, Charsets.UTF_8, false);
  }

  /** Creates a  over a given file, assuming a UTF-8 encoding. */
  public FileLineIterable(File file, boolean skipFirstLine) throws IOException {
    this(file, Charsets.UTF_8, skipFirstLine);
  }
  
  /** Creates a  over a given file, using the given encoding. */
  public FileLineIterable(File file, Charset encoding, boolean skipFirstLine) throws IOException {
    this(FileLineIterator.getFileInputStream(file), encoding, skipFirstLine);
  }

  public FileLineIterable(InputStream is) {
    this(is, Charsets.UTF_8, false);
  }
  
  public FileLineIterable(InputStream is, boolean skipFirstLine) {
    this(is, Charsets.UTF_8, skipFirstLine);
  }
  
  public FileLineIterable(InputStream is, Charset encoding, boolean skipFirstLine) {
    this.is = is;
    this.encoding = encoding;
    this.skipFirstLine = skipFirstLine;
    this.origFilename = "";
  }

  public FileLineIterable(InputStream is, Charset encoding, boolean skipFirstLine, String filename) {    
    this.is = is;
    this.encoding = encoding;
    this.skipFirstLine = skipFirstLine;
    this.origFilename = filename;
  }
  
  
  @Override
  public Iterator<String> iterator() {
    try {
      return new FileLineIterator(is, encoding, skipFirstLine, this.origFilename);
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }
  
}
