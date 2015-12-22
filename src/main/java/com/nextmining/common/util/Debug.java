package com.nextmining.common.util;

/**
 * Debugging utilities to print out.
 * 
 * @author Younggue Bae
 * 
 */
public class Debug {

	public final static int OFF = 9;
	public final static int SEVERE = 4;
	public final static int WARNING = 3;
	public final static int INFO = 2;
	public final static int FINE = 1;
	public final static int ALL = 0;

	private final static int LEVEL = 2;

	/**
	 * Prints a message to debug.
	 * 
	 * @param level
	 * @param message
	 */
	public static final void println(int level, Object message) {
		if (level >= LEVEL) {
			if (level > INFO) {
				System.err.println(message);
			} else {
				System.out.println(message);
			}
		}
	}

}
