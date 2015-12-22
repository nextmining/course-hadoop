package com.nextmining.common.util;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for generating features from a dictionary.
 * 
 * @author Younggue Bae
 */
public class RegexDictionary {

	/** Mapping associating features to regular expression patterns. */
	private Map<String, Pattern> map;
	
	public RegexDictionary(String... filenames) throws IOException {
		map = new HashMap<String, Pattern>();
		for (String filename : filenames) {
			try {
				Map<String, Pattern> dicMap = loadDictionary(filename);
				map.putAll(dicMap);
				System.err.println("Feature dictionary loaded (" + dicMap.size() + " feature categories)");
			} catch (IOException e) {
				System.err.println("Error: file " + filename + " doesn't exist");
				e.printStackTrace();
				throw e;
			} catch (NullPointerException e) {
				System.err.println("Error: feature dicitonary file " + filename + " doesn't have the right format");
				e.printStackTrace();
				throw e;
			}
		}
	}

	public RegexDictionary(String filename) throws IOException {
		try {
			map = loadDictionary(filename);
			System.err.println("Feature dictionary loaded (" + map.size() + " feature categories)");
		} catch (IOException e) {
			System.err.println("Error: file " + filename + " doesn't exist");
			e.printStackTrace();
			throw e;
		} catch (NullPointerException e) {
			System.err.println("Error: feature dicitonary file " + filename + " doesn't have the right format");
			e.printStackTrace();
			throw e;
		}
	}

	private Map<String, Pattern> loadDictionary(String filename) throws IOException {
		BufferedReader in = null;
		InputStream is = getClass().getResourceAsStream(filename);

		if (is != null) {
			in = new BufferedReader(new InputStreamReader(is, "utf-8"));
		} else {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));
		}

		Map<String, Pattern> wordLists = new LinkedHashMap<String, Pattern>();
		String category = "";
		String currentVariable = "";
		String catRegex = "";
		int wordCount = 0;

		boolean embraceWordBoundary = false;
		String line;
		while ((line = in.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}
			// parse parameters
			if (line.startsWith("@")) {
				String param = line.split("=")[0].trim().replaceAll("@", "");
				String value = line.split("=")[1].trim();
				if (param.equalsIgnoreCase("EMBRACE_WORD_BOUNDARY")) {
					embraceWordBoundary = Boolean.parseBoolean(value);
				}
			}
			// right trim
			line = line.replaceAll("\\s+$", "");

			// if encounter new category
			if (line.matches("^[\\S]+")) {
				// add full regex to database
				if (!catRegex.equals("")) {
					catRegex = catRegex.substring(0, catRegex.length() - 1);
					catRegex = "(" + catRegex + ")";
					wordLists.put(category + "." + currentVariable, Pattern.compile(catRegex));
				}
				// update variable
				currentVariable = "";
				catRegex = "";

				// update category
				category = line.trim();
			}
			// if encounter new variable
			else if (line.matches("\\t[\\S]+")) {
				// add full regex to database
				if (!catRegex.equals("")) {
					catRegex = catRegex.substring(0, catRegex.length() - 1);
					//catRegex = catRegex.replaceAll("\\*", "[.]*");
					catRegex = "(" + catRegex + ")";
					wordLists.put(category + "." + currentVariable, Pattern.compile(catRegex));
				}
				// update variable
				currentVariable = line.split("\t")[1];
				catRegex = "";

			} 
			// if encounter dictionary word
			else if (line.matches("\\t\\t.+")) {
				wordCount++;
				String newPattern = line.split("\\s+")[1];
	
				if (embraceWordBoundary) {
					catRegex += "\\b" + newPattern + "\\b|";
				}
				else {
					catRegex += newPattern + "|";
				}
			}
		}

		// add last regex to database
		if (!catRegex.equals("")) {
			catRegex = catRegex.substring(0, catRegex.length() - 1);
			catRegex = "(" + catRegex + ")";
			wordLists.put(category + "." + currentVariable, Pattern.compile(catRegex));
		}

		in.close();

		System.out.println(wordCount + " words and " + wordLists.size() + " categories loaded in feature dictionary");
		System.out.println(wordLists);
		return wordLists;
	}

	public Map<String, Double> getCounts(String text) {

		Map<String, Double> counts = new LinkedHashMap<String, Double>(map.size());

		// System.out.println("input = " + text);

		// first get all lexical counts
		for (String cat : map.keySet()) {

			// add entry to output hash
			Pattern catRegex = map.get(cat);
			int catCount = 0;

			Matcher m = catRegex.matcher(text);
			while (m.find()) {
				catCount++;
			}

			if (catCount > 0) {
				counts.put(cat, new Double(catCount));
			}
		}

		return counts;
	}

	public Map<String, Double> sort(Map<String, Double> map, boolean ascending) {
		List<String> mapKeys = new ArrayList<String>(map.keySet());
		List<Double> mapValues = new ArrayList<Double>(map.values());

		Collections.sort(mapValues, new CountComparator(ascending));
		Collections.sort(mapKeys);

		LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();

		Iterator<Double> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Object val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				Object key = keyIt.next();
				String comp1 = map.get(key).toString();
				String comp2 = val.toString();

				if (comp1.equals(comp2)) {
					map.remove(key);
					mapKeys.remove(key);
					sortedMap.put((String) key, (Double) val);
					break;
				}
			}
		}
		return sortedMap;
	}

	class CountComparator implements Comparator<Double> {
		private boolean ascending = true;

		public CountComparator(boolean ascending) {
			this.ascending = ascending;
		}

		public int compare(Double o1, Double o2) {
			if (ascending)
				return o1.compareTo(o2);
			else
				return o2.compareTo(o1);
		}
	}

}
