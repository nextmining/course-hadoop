package com.nextmining.nlp;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Created by louie on 2016. 1. 2..
 */
public class NLPToolsTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void testAnalyzeSentence() throws Exception {
        NLPTools nlp = new NLPTools();

        String text = "This app worked perfectly for me for over a year but after I did the most recent update I stopped being able to send messages as did the person I had been chatting with. Please fix this LINE!!!!";

        Sentence sentence = nlp.analyzeSentence(text);
        sentence.printTokens();

        List<Token> tokens = sentence.getKeywordTokens();

        System.out.println("\n===========================================");
        for (Token token : tokens) {
            System.out.println(token);
        }
    }
}
