package com.nextmining.common.job;

import com.nextmining.common.util.FileUtil;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.IOException;
import java.util.Date;

/**
 * This class is a job logger.
 * 
 * @author Younggue Bae
 */
public class JobLogger {
	
	static JobLogger instance = null;
	static Logger logger = null;
	String layout = "[%d{HH:mm:ss}]	%m%n";
	String datePattern = ".yyyy-MM-dd";
	PatternLayout patternLayout = new PatternLayout(layout);
	DailyRollingFileAppender appender = null;

	/**
	 * Constructor
	 * 
	 * @param name the logger name
	 * @param logFile the logger file name
	 */
	public JobLogger(Class<?> name, String logFile) {
		try {
			FileUtil.mkdirsFromFullpath(logFile);
			
			logger = Logger.getLogger(name);
			appender = new DailyRollingFileAppender(patternLayout, logFile, datePattern);
			logger.addAppender(appender);
			logger.setLevel(Level.ALL);
		} catch (IOException ie) {
			ie.printStackTrace();
		}		
	}
	
	/**
	 * Gets the job logger instance.
	 * 
	 * @param name the logger name
	 * @param logFile the logger file name
	 * @return JobLogger the job logger instance
	 */
	public static JobLogger getLogger(Class<?> name, String logFile) {
		if (instance == null) {
			instance = new JobLogger(name, logFile);
		}		
		return instance;
	}

	/**
	 * Writes the info message.
	 * 
	 * @param message the message
	 */
	public void info(String message) {
		//System.out.println(message);
		logger.info(message);
	}
	
	/**
	 * Writes the debug message.
	 * 
	 * @param message the message
	 */
	public void debug(String message) {
		//System.out.println(message);
		logger.debug(message);
	}
	
	public void info(String message, Object[] params) {
		
	}
	
	/**
	 * Writes the error message.
	 * 
	 * @param message the message
	 * @param t the exception
	 */
	public void error(String message, Throwable t) {
		logger.error(message, t);
	}

	/**
	 * Writes the job summary log.
	 * 
	 * @param message the message
	 * @param startTime the startTime
	 * @param endTime the endTime	 * 
	 */
	public void jobSummary(String message, Date startTime, Date endTime) {
		long time = endTime.getTime() - startTime.getTime();		
		String elapsedTime = (int)Math.floor(time/1000)/60 + "min" + " " + (int)Math.floor(time/1000) % 60 +"sec";
		
		logger.info("==========================================");
		logger.info(message);
		logger.info("Start Time : " + startTime);
		logger.info("End Time : " + endTime);
		logger.info("Elapsed Time : " + elapsedTime);
		logger.info("==========================================");		
	}

}
