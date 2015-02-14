package com.rv.tools;

import junit.framework.TestCase;


public class CorrectSentenceTest extends TestCase {
	
	public void testSentenceWhithComma(){
		final String sentence = "ceci est la phrase. la phrase 2 est pas mal.La 3 également.ce test est cool. Mais il est déjà fini.";
		org.junit.Assert.assertEquals("Ceci est la phrase. La phrase 2 est pas mal. La 3 également. Ce test est cool. Mais il est déjà fini.", CorrectSentence.punctuation(sentence));
	}
	
	public void testSentenceWhithLotCommas(){
		final String sentence = "...";
		org.junit.Assert.assertEquals("...", CorrectSentence.punctuation(sentence));
	}
	
	public void testSentenceNull(){
		org.junit.Assert.assertNull(CorrectSentence.punctuation(null));
	}
	
	public void testSentenceWhithConsecutiveCommas(){
		final String sentence = "ceci est la phrase...la phrase 2 est pas mal..La 3 également.End";
		org.junit.Assert.assertEquals("Ceci est la phrase... La phrase 2 est pas mal.. La 3 également. End.", CorrectSentence.punctuation(sentence));
	}
}
