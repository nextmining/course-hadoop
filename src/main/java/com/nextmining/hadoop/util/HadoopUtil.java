package com.nextmining.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;

/**
 * This class is Hadoop utilities.
 * 
 * @author Younggue Bae
 */
public final class HadoopUtil {

  private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);

  private HadoopUtil() { }

  /**
   * Create a map-only Hadoop Job out of the passed in parameters.  Does not set the
   * Job name.
   *
   * @see #getCustomJobName(String, org.apache.hadoop.mapreduce.JobContext, Class, Class)
   */
  @SuppressWarnings("rawtypes")
	public static Job prepareJob(Path inputPath,
                                 Path outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends OutputFormat> outputFormat, Configuration conf) throws IOException {

    //Job job = new Job(new Configuration(conf));
  	Job job = Job.getInstance(conf);
    Configuration jobConf = job.getConfiguration();

    if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
    }
    job.setJarByClass(mapper);

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);
    job.setOutputKeyClass(mapperKey);
    job.setOutputValueClass(mapperValue);
    jobConf.setBoolean("mapred.compress.map.output", true);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  /**
   * Create a map and reduce Hadoop job.  Does not set the name on the job.
   * @param inputPath The input {@link org.apache.hadoop.fs.Path}
   * @param outputPath The output {@link org.apache.hadoop.fs.Path}
   * @param inputFormat The {@link org.apache.hadoop.mapreduce.InputFormat}
   * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
   * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
   * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
   * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
   * @param reducerKey The reducer key class.
   * @param reducerValue The reducer value class.
   * @param outputFormat The {@link org.apache.hadoop.mapreduce.OutputFormat}.
   * @param conf The {@link org.apache.hadoop.conf.Configuration} to use.
   * @return The {@link org.apache.hadoop.mapreduce.Job}.
   * @throws IOException if there is a problem with the IO.
   *
   * @see #getCustomJobName(String, org.apache.hadoop.mapreduce.JobContext, Class, Class)
   * @see #prepareJob(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class, org.apache.hadoop.conf.Configuration)
   */
  @SuppressWarnings("rawtypes")
	public static Job prepareJob(Path inputPath,
                                 Path outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue,
                                 Class<? extends OutputFormat> outputFormat,
                                 Configuration conf) throws IOException {

    //Job job = new Job(new Configuration(conf));
  	Job job = Job.getInstance(conf);
  		
    Configuration jobConf = job.getConfiguration();

    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    if (mapperKey != null) {
      job.setMapOutputKeyClass(mapperKey);
    }
    if (mapperValue != null) {
      job.setMapOutputValueClass(mapperValue);
    }

    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }


  @SuppressWarnings("rawtypes")
	public static String getCustomJobName(String className, JobContext job,
                                  Class<? extends Mapper> mapper,
                                  Class<? extends Reducer> reducer) {
    StringBuilder name = new StringBuilder(100);
    String customJobName = job.getJobName();
    if (customJobName == null || customJobName.trim().isEmpty()) {
      name.append(className);
    } else {
      name.append(customJobName);
    }
    name.append('-').append(mapper.getSimpleName());
    if (reducer != null) {
    	name.append('-').append(reducer.getSimpleName());
    }
    return name.toString();
  }


  public static void delete(Configuration conf, Iterable<Path> paths) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(conf);
      if (fs.exists(path)) {
        log.info("Deleting {}", path);
        fs.delete(path, true);
      }
    }
  }

  public static void delete(Configuration conf, Path... paths) throws IOException {
    delete(conf, Arrays.asList(paths));
  }

  public static InputStream openStream(Path path, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.open(path.makeQualified(fs));
  }

  public static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
    try {
      return fs.listStatus(path);
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  public static FileStatus[] listStatus(FileSystem fs, Path path, PathFilter filter) throws IOException {
    try {
      return fs.listStatus(path, filter);
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  public static void cacheFiles(Path fileToCache, Configuration conf) {
    DistributedCache.setCacheFiles(new URI[]{fileToCache.toUri()}, conf);
  }

  public static Path cachedFile(Configuration conf) throws IOException {
    return new Path(DistributedCache.getCacheFiles(conf)[0].getPath());
  }

  public static void setSerializations(Configuration conf) {
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
  }

  public static void writeInt(int value, Path path, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FSDataOutputStream out = fs.create(path);
    try {
      out.writeInt(value);
    } finally {
      out.close();
    }
  }

  public static int readInt(Path path, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    //FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream in = fs.open(path);
    try {
      return in.readInt();
    } finally {
      in.close();
    }
  }

}
