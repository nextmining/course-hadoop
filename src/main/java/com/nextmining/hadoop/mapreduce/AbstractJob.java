package com.nextmining.hadoop.mapreduce;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Superclass of many Hadoop "jobs". A job drives configuration and launch of
 * one or more maps and reduces in order to accomplish some task.
 * 
 * @author Younggue Bae
 */
public abstract class AbstractJob extends Configured implements Tool {
	
	protected static final Logger logger = LoggerFactory.getLogger(AbstractJob.class);
	
	/* command line options */
	private Options options;

	/* parsed command line */
	private CommandLine cmd;

	protected AbstractJob() {
		options = new Options();
	}

	/**
	 * Add an option with no argument whose presence can be checked for using {@code containsKey}
	 * method on the map returned by {@link #parseArguments(String[])};
	 */
	protected void addFlag(String name, String shortName, String description) {
		options.addOption(buildOption(name, shortName, description, false, false));
	}

	/**
	 * Add an option to the the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called. This options has an argument with null as its
	 * default value.
	 */
	protected void addOption(String name, String shortName, String description) {
		options.addOption(buildOption(name, shortName, description, true, false));
	}

	/**
	 * Add an option to the the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called.
	 * 
	 * @param required
	 *          if true the {@link #parseArguments(String[])} will throw fail with an error and usage
	 *          message if this option is not specified on the command line.
	 */
	protected void addOption(String name, String shortName, String description, boolean required) {
		options.addOption(buildOption(name, shortName, description, true, required));
	}

	/**
	 * Add an arbitrary option to the set of options this job will parse when
	 * {@link #parseArguments(String[])} is called. If this option has no argument, use
	 * {@code containsKey} on the map returned by {@code parseArguments} to check for its presence.
	 * Otherwise, the string value of the option will be placed in the map using a key equal to this
	 * options long name preceded by '--'.
	 * 
	 * @return the option added.
	 */
	protected Option addOption(Option option) {
		options.addOption(option);
		return option;
	}

	/**
	 * Build an option with the given parameters. Name and description are required.
	 * 
	 * @param name
	 *          the long name of the option prefixed with '--' on the command-line
	 * @param shortName
	 *          the short name of the option, prefixed with '-' on the command-line
	 * @param description
	 *          description of the option displayed in help method
	 * @param hasArg
	 *          true if the option has an argument.
	 * @param required
	 *          true if the option is required.
	 * @return the option.
	 */
	@SuppressWarnings("static-access")
	protected static Option buildOption(String name, String shortName, String description,
			boolean hasArg, boolean required) {

		Option option = OptionBuilder.withArgName(shortName).withLongOpt(name).isRequired(required)
				.hasArg(hasArg).withDescription(description).create();

		return option;
	}

	/**
	 * Returns a default command line option for help. Used by all clustering jobs and many others
	 * */
	@SuppressWarnings("static-access")
	public static Option helpOption() {
		return OptionBuilder.withArgName("h").withLongOpt("help").isRequired(false).hasArg(false)
				.withDescription("Print out help").create();
	}

	/**
	 * Parse the arguments specified based on the options defined using the various {@code addOption}
	 * methods.
	 */
	public void parseArguments(String[] args) throws IOException {

		options.addOption(helpOption());

		try {
			CommandLineParser parser = new PosixParser();
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Job-Specific Options:", options);
			
			System.exit(1);
		}

		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Job-Specific Options:", options);
			System.exit(1);
		}
	}

	/**
	 * @return the requested option, or null if it has not been specified
	 */
	public String getOption(String optionName) {
		return cmd.getOptionValue(optionName);
	}

	/**
	 * Get the option, else the default
	 * 
	 * @param optionName
	 *          The name of the option to look up, without the --
	 * @param defaultVal
	 *          The default value.
	 * @return The requested option, else the default value if it doesn't exist
	 */
	public String getOption(String optionName, String defaultVal) {
		String res = getOption(optionName);
		if (res == null) {
			res = defaultVal;
		}
		return res;
	}

	/**
	 * @return if the requested option has been specified
	 */
	public boolean hasOption(String optionName) {
		return options.hasOption(optionName);
	}
	
	@Override
	public abstract int run(String[] args) throws Exception;

}
