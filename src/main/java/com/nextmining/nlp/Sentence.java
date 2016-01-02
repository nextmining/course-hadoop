package com.nextmining.nlp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * This class defines a sentence that consits of token list.
 * 
 * @author Younggue Bae
 */
@SuppressWarnings("serial")
public class Sentence extends ArrayList<Token> {
	
	/**
	 * Detokenizes tokens.
	 * 
	 * @param startIndex
	 * @param endIndex
	 * @return
	 */
	public String detokenize(int startIndex, int endIndex) {
		List<Token> tokens = this.subList(startIndex, endIndex);
		
		StringBuffer sb = new StringBuffer();
		for (Token token : tokens) {
			sb.append(token.getToken()).append(" ");
		}
		
		return sb.toString().trim();
	}
	
	/**
	 * Gets the keyword tokens.
	 *
	 * @return
	 */
	public List<Token> getKeywordTokens() {
		List<Token> tokens = new ArrayList<Token>();
		
		String[] availPosTags = { 
				"CD",	// cardinal number
				"FW",	// foreign word
				"JJ",	// adjective
				"JJR",	// adj., comparative
				"JJS",	// adj., superlative
				"NN",	// noun, sing. or mass
				"NNS",	// noun, plural
				"NNP",	// proper noun, singular
				"NNPS",	// proper noun, plural
				"VB",	// verb, base form
				"VBD",	// verb, preterite(past tense)
				"VBG",	// verb, gerund
				"VBN",	// verb, past participle
				"VBP",	// verb, non-3sg pres
				"VBZ",	// verb, 3sg pres
                "RB"    // adverbs
        };
		
		for (Token token : this) {
			String posTag = token.getPosTag();
			
			if (Arrays.asList(availPosTags).contains(posTag)) {
				int startPos = token.getSpan().getStart();
				int endPos = token.getSpan().getEnd();

				tokens.add(token);
			}
		}
		
		return tokens;
	}
	
	/**
	 * Prints tokens.
	 */
	public void printTokens() {
		StringBuffer header = new StringBuffer()
			.append("index").append("\t")
			.append("token").append("\t")
			.append("start").append("\t")
			.append("end").append("\t")
			.append("pos_tag").append("\t")
			.append("chunk_tag").append("\t")
			.append("attributes");
		
		System.out.println("---------------------------------------------------");
		System.out.println("\t\t All Tokens");
		System.out.println(header.toString());
		System.out.println("---------------------------------------------------");
		
		for (Token token : this) {
			System.out.println(token.toString());
		}
	}

}
