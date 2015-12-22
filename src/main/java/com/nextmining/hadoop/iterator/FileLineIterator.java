package com.nextmining.hadoop.iterator;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * Iterates over the lines of a text file. This assumes the text file's lines are delimited in a manner
 * consistent with how {@link BufferedReader} defines lines.
 * <p/>
 * This class will uncompress files that end in .zip or .gz accordingly, too.
 */
public final class FileLineIterator extends AbstractIterator<String> implements SkippingIterator<String>, Closeable {

  private final BufferedReader reader;

  private static final Logger log = LoggerFactory.getLogger(FileLineIterator.class);

  /**
   * Creates a  over a given file, assuming a UTF-8 encoding.
   *
   * @throws java.io.FileNotFoundException if the file does not exist
       * @throws IOException
       *           if the file cannot be read
       */

  public FileLineIterator(File file) throws IOException {
    this(file, Charsets.UTF_8, false);
  }

  /**
   * Creates a  over a given file, assuming a UTF-8 encoding.
   *
   * @throws java.io.FileNotFoundException if the file does not exist
   * @throws IOException                   if the file cannot be read
   */
  public FileLineIterator(File file, boolean skipFirstLine) throws IOException {
    this(file, Charsets.UTF_8, skipFirstLine);
  }

  /**
   * Creates a  over a given file, using the given encoding.
   *
   * @throws java.io.FileNotFoundException if the file does not exist
   * @throws IOException                   if the file cannot be read
   */
  public FileLineIterator(File file, Charset encoding, boolean skipFirstLine) throws IOException {
    this(getFileInputStream(file), encoding, skipFirstLine);
  }

  public FileLineIterator(InputStream is) throws IOException {
    this(is, Charsets.UTF_8, false);
  }

  public FileLineIterator(InputStream is, boolean skipFirstLine) throws IOException {
    this(is, Charsets.UTF_8, skipFirstLine);
  }

  public FileLineIterator(InputStream is, Charset encoding, boolean skipFirstLine) throws IOException {
    reader = new BufferedReader(new InputStreamReader(is, encoding));
    if (skipFirstLine) {
      reader.readLine();
    }
  }

  public FileLineIterator(InputStream is, Charset encoding, boolean skipFirstLine, String filename)
    throws IOException {
    InputStream compressedInputStream;

    if ("gz".equalsIgnoreCase(Files.getFileExtension(filename.toLowerCase()))) {
      compressedInputStream = new GZIPInputStream(is);
    } else if ("zip".equalsIgnoreCase(Files.getFileExtension(filename.toLowerCase()))) {
      compressedInputStream = new ZipInputStream(is);
    } else {
      compressedInputStream = is;
    }

    reader = new BufferedReader(new InputStreamReader(compressedInputStream, encoding));
    if (skipFirstLine) {
      reader.readLine();
    }
  }

  static InputStream getFileInputStream(File file) throws IOException {
    InputStream is = new FileInputStream(file);
    String name = file.getName();
    if ("gz".equalsIgnoreCase(Files.getFileExtension(name.toLowerCase()))) {
      return new GZIPInputStream(is);
    } else if ("zip".equalsIgnoreCase(Files.getFileExtension(name.toLowerCase()))) {
      return new ZipInputStream(is);
    } else {
      return is;
    }
  }

  @Override
  protected String computeNext() {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException ioe) {
      try {
        close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      throw new IllegalStateException(ioe);
    }
    return line == null ? endOfData() : line;
  }


  @Override
  public void skip(int n) {
    try {
      for (int i = 0; i < n; i++) {
        if (reader.readLine() == null) {
          break;
        }
      }
    } catch (IOException ioe) {
      try {
        close();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    endOfData();
    Closeables.close(reader, true);
  }
}
