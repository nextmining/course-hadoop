package com.nextmining.nlp;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * This class is a NLP tools based on Apache OpenNLP.
 * 
 * @author Younggue Bae
 */
public class NLPTools {
	
	/** tokenizer */
	private Tokenizer tokenizer;
	
	/** pos tagger */
	private POSTaggerME tagger;

	/** a singleton instance */
	private static NLPTools instance;
	
	/**
	 * Constructor.
	 */
	public NLPTools() {
		try {
			tokenizer = initTokenizer("/model/en-token.bin");
			tagger = initPosTagger("/model/en-pos-maxent.bin");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	/**
	 * Gets a singleton instance.
	 * 
	 * @return
	 */
	public static NLPTools getInstance() {
		if (instance == null) {
			instance = new NLPTools();
		}
		
		return instance;
	}
	
	private Tokenizer initTokenizer(String modelFile) {
		InputStream modelIn = null;

		try {
			//modelIn = new FileInputStream(modelFile);
			modelIn = getClass().getResourceAsStream(modelFile);
			TokenizerModel model = new TokenizerModel(modelIn);
			Tokenizer tokenizer = new TokenizerME(model);
			
			return tokenizer;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (modelIn != null) {
				try {
					modelIn.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}
	
	private POSTaggerME initPosTagger(String modelFile) {
		InputStream modelIn = null;

		try {
			//modelIn = new FileInputStream(modelFile);
			modelIn = getClass().getResourceAsStream(modelFile);
			POSModel model = new POSModel(modelIn);
			POSTaggerME tagger = new POSTaggerME(model);
			
			return tagger;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (modelIn != null) {
				try {
					modelIn.close();
				} catch (IOException e) {
				}
			}
		}	
		return null;
	}

	/**
	 * Tokenizes a sentence.
	 * 
	 * @param text
	 * @return
	 */
	public String[] tokenize(String text) {
		String[] tokens = tokenizer.tokenize(text);

		return tokens;
	}
	
	/**
	 * Tokenizes a sentence with span.
	 * 
	 * @param text
	 * @return
	 */
	public Span[] tokenizePos(String text) {
		Span[] span = tokenizer.tokenizePos(text);
		
		return span;
	}
	
	/**
	 * Tags pos.
	 * 
	 * @param tokens
	 * @return
	 */
	public String[] tagPos(String[] tokens) {
		String[] tags = tagger.tag(tokens);

		return tags;
	}

	/**
	 * Analyzes a text to a sentence.
	 * 
	 * @param text
	 * @return
	 * @throws Exception
	 */
	public Sentence analyzeSentence(String text) throws Exception {
		Sentence sentence = new Sentence();
		
		String[] tokens = tokenize(text);
		Span[] tokenSpan = tokenizePos(text);
		String[] posTags = tagPos(tokens);
		
		for (int index = 0; index < tokens.length; index++) {
			Token token = new Token();
			token.setIndex(index);
			token.setSpan(tokenSpan[index]);
			token.setToken(tokens[index]);
			token.setPosTag(posTags[index]);
			
			sentence.add(token);
		}
		
		return sentence;
	}
	
	private String detokenize(String[] tokens, int startIndex, int endIndex) {
		List<String> subTokens = Arrays.asList(tokens).subList(startIndex, endIndex);
		
		StringBuffer sb = new StringBuffer();
		for (String token : subTokens) {
			sb.append(token).append(" ");
		}
		
		return sb.toString().trim();
	}

}
