package com.nextmining.nlp;

import java.util.HashMap;
import java.util.Map;

import opennlp.tools.util.Span;

/**
 * This class is a token.
 * 
 * @author Younggue Bae
 */
public class Token {

	public int index;
	public Span span;
	public String token;
	public String posTag;
	public String chunkTag;
	public Map<String, Object> attributes = new HashMap<String, Object>();
	
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public Span getSpan() {
		return span;
	}
	public void setSpan(Span span) {
		this.span = span;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	public String getPosTag() {
		return posTag;
	}
	public void setPosTag(String posTag) {
		this.posTag = posTag;
	}
	public String getChunkTag() {
		return chunkTag;
	}
	public void setChunkTag(String chunkTag) {
		this.chunkTag = chunkTag;
	}
	public Map<String, Object> getAttributes() {
		return attributes;
	}
	public void setAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}
	
	public void addAttribute(String name, Object value) {
		attributes.put(name, value);
	}
	
	public Object getAttribute(String name) {
		return attributes.get(name);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer()
			.append(index).append("\t")
			.append(token).append("\t")
			.append(span.getStart()).append("\t")
			.append(span.getEnd()).append("\t")
			.append(posTag).append("\t")
			.append(chunkTag).append("\t")
			.append(attributes);

		return sb.toString();
	}

}
