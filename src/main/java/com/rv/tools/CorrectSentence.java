package com.rv.tools;


public class CorrectSentence {

	public static String punctuation(String sentence){
		
		if (sentence == null) return null;
		
		final String[] splits = sentence.split("\\.");
		String result = "";
		if (splits.length > 0){
			for (String split : splits){
				final String current = split.trim();
				String toAdd="";
				if (current.length()>1) {
					toAdd = " " + current.substring(0,1).toUpperCase() + current.substring(1) + ".";
				}else {
					toAdd = current + ".";
				}
				result = result + toAdd;
			}
		} else result = sentence;
		return result.trim();
	}
}
