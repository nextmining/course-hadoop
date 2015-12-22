package com.nextmining.common.job;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Round Robin based history buffer to check duplication data.
 * 
 * @author Younggue Bae
 */
public class RoundHistoryBuffer {
	
	private BufferedWriter br;
	private int maxRound = 1;
	private int currentRound = 1;
	private Set<String> historySet = new HashSet<String>();
	private Set<String> historyRoundSet = new LinkedHashSet<String>();
	private static final String DELIMITER = "\t";
	
	public RoundHistoryBuffer(String file, int maxRound) {
		this.maxRound = maxRound;
		
		try {
			this.loadHistory(new File(file));
			this.br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), "UTF-8"));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} 
	}
	
	private void loadHistory(File file) throws IOException {
		historySet = new HashSet<String>();
		historyRoundSet = new LinkedHashSet<String>();
		
		try {			
			InputStream is = new FileInputStream(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8")); 
			
			int latestRound = 0;
			String line;
			while ((line = in.readLine()) != null) {
				String[] tokens = Pattern.compile(DELIMITER).split(line);	
				int round = Integer.valueOf(tokens[0]);		
				String id = tokens[1];						
				
				historySet.add(id);
				historyRoundSet.add(String.valueOf(round) + DELIMITER + id);
				
				if (round > latestRound)
					latestRound = round;
			}
			is.close();				
			currentRound = latestRound + 1;
			
			System.out.println("current round == " + currentRound);
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void close() throws IOException {
		br.close();
	}
	
	public boolean checkDuplicate(String newId) {
		return historySet.contains(newId);
	}
	
	public void writeHistory(Set<String> idSet) throws IOException {			
		this.rewritePrevHistory(historyRoundSet);
		
		int round = 1;
		if(currentRound <= maxRound)
			round = currentRound;
		else
			round = currentRound - 1;
		
		for (Iterator<String> it = idSet.iterator(); it.hasNext();) {
			br.write(round + DELIMITER + it.next());
			br.newLine();
		}
		
		br.close();		
	}
	
	private void rewritePrevHistory(Set<String> list)
			throws IOException {
		if (currentRound <= maxRound) {
			for (Iterator<String> it = list.iterator(); it.hasNext();) {
				br.write(it.next());
				br.newLine();
			}
		} else {
			
			for (Iterator<String> it = list.iterator(); it.hasNext();) {
				String line = it.next();
				String[] tokens = Pattern.compile(DELIMITER).split(line);	
				int round = Integer.valueOf(tokens[0]);		
				String id = tokens[1];
				if (round > 1) {
					br.write((round - 1) + DELIMITER + id);
					br.newLine();					
				}
			}
		}
	}
	
}
