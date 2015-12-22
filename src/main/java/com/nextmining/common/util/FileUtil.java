package com.nextmining.common.util;

import java.io.*;

/**
 * This class is file utilities.
 * 
 * @author Younggue Bae
 */
public class FileUtil {
	
	/**
	 * Extracts directory path from full filename path.
	 * 
	 * @param filename
	 * @return
	 */
	public static final String extractDirectory(String filename) {
		if (StringUtil.isEmpty(filename))
			return null;
		
		int end = filename.lastIndexOf(File.separator);
		if (end >= 0)
			return filename.substring(0, end);
		else
			return null;
	}
	
	/**
	 * Makes directory from full filename path.
	 * 
	 * @param filename
	 */
	public static final void mkdirsFromFullpath(String filename) {
		String strDir = extractDirectory(filename);
		if (!StringUtil.isEmpty(strDir)) {
			File dir = new File(strDir);
			if (!dir.exists()) {
				dir.mkdirs();
			}
		}
	}
	
	/**
	 * Makes directory.
	 * 
	 * @param filename
	 */
	public static final void mkdirs(String strDir) {
		if (!StringUtil.isEmpty(strDir)) {
			File dir = new File(strDir);
			if (!dir.exists()) {
				dir.mkdirs();
			}
		}
	}
	
	public static final void copy(String source, String target) throws IOException {
		InputStream inStream = null;
		OutputStream outStream = null;

		try {

			File afile = new File(source);
			File bfile = new File(target);
			
			if (!afile.exists())	return;

			inStream = new FileInputStream(afile);
			outStream = new FileOutputStream(bfile);

			byte[] buffer = new byte[1024];

			int length;
			// copy the file content in bytes
			while ((length = inStream.read(buffer)) > 0) {
				outStream.write(buffer, 0, length);
			}

			inStream.close();
			outStream.close();

			System.out.println("File is copied successfully!");

		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

}
