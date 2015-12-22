package com.nextmining.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * HDFS Client.
 * 
 * @author Younggue Bae
 *
 */
public class HDFSClient {

	private Configuration conf;
	
	public HDFSClient(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * Imports a local source file to HDFS target path.
	 * 
	 * @param sourceFile
	 * @param targetPath
	 * @throws IOException
	 */
	public void importFile(String sourceFile, String targetPath) throws IOException {

		InputStream in = new BufferedInputStream(new FileInputStream(sourceFile));
		System.out.println("Start to import a file into " + targetPath);

		FileSystem fs = FileSystem.get(URI.create(targetPath), conf);
		OutputStream out = fs.create(new Path(targetPath), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});

		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	/**
	 * Merges local source files into a merged file and imports this file to HDFS
	 * target path.
	 * 
	 * @param sourceDir
	 * @param targetPath
	 * @throws IOException
	 */
	public void importMergeFiles(String sourceDir, String targetPath) throws IOException {
		String[] sourceDirs = { sourceDir };
		this.importMergeFiles(sourceDirs, targetPath);
	}
	
	/**
	 * Merges local source files into a merged file and imports this file to HDFS
	 * target path.
	 * 
	 * @param sourceDirs
	 * @param targetPath
	 * @throws IOException
	 */
	public void importMergeFiles(String[] sourceDirs, String targetPath) throws IOException {

		System.out.println("Start to import a merged file into " + targetPath);

		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		int filesProcessed = 0;

		Path hdfsFile = new Path(targetPath);
		List<FileStatus> sourceFileList = new ArrayList<FileStatus>();
		for (String sourceDir : sourceDirs) {
			Path sourcePath = new Path(sourceDir);
			FileStatus[] sourceFiles = local.listStatus(sourcePath);
			sourceFileList.addAll(Arrays.asList(sourceFiles));
		}

		// delete already exist file in hdfs
		if (hdfs.exists(hdfsFile))
			hdfs.delete(hdfsFile, true);

		FSDataOutputStream out = hdfs.create(hdfsFile, new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});

		for (FileStatus sourceFile : sourceFileList) {
			if (!sourceFile.isDir()) {
				System.out.println("\tnow processing <" + sourceFile.getPath().getName() + ">");
				FSDataInputStream in = local.open(sourceFile.getPath());

				byte buffer[] = new byte[4096];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				filesProcessed++;
				in.close();
			}
		}
		out.close();
		System.out.println("\nSuccessfully merged " + filesProcessed + " local files and written to <" + hdfsFile.getName()
				+ "> in HDFS.");
	}

	/**
	 * Exports a source file in HDFS into a local target file.
	 * 
	 * @param sourcePath
	 * @param targetFile
	 * @throws IOException
	 */
	public void exportFile(String sourcePath, String targetFile) throws IOException {

		com.nextmining.common.util.FileUtil.mkdirsFromFullpath(targetFile);

		System.out.println("Start to export hdfs file from " + sourcePath);

		FileSystem fs = FileSystem.get(URI.create(sourcePath), conf);
		FileOutputStream out = new FileOutputStream(targetFile, false);
		InputStream in = null;

		try {
			in = fs.open(new Path(sourcePath));
			IOUtils.copyBytes(in, out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
			out.close();
		}

		System.out.println("Finish to export hdfs file into " + targetFile);
	}
	
}
