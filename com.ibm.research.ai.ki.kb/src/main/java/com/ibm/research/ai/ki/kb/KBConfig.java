package com.ibm.research.ai.ki.kb;

import java.io.*;

import com.ibm.research.ai.ki.util.*;

public class KBConfig extends PropertyStruct {
    private static final long serialVersionUID = 1L;

    public String kbDir;
    
    /**
     * To avoid generic terms, we ignore terms that occur more than this many times.
     */
    public int maxNodeCorpusCount = 3000000;
    /**
     * We can ignore rare terms if desired.
     */
    public int minNodeCorpusCount = 1;
    /**
     * 
     */
    public int minUnaryCount = 100;
    /**
     * Whether to consider super-relations in the labels for context sets.
     */
    public boolean useRelationTaxonomy = true;
    
    //for the coarse-grained type system
    /**
     * A type must have this many instances for which it is the most specific type
     */
    public int minTypeSize = 3000;
    /**
     * We will have no more than this many types in the coarse grained type system
     */
    public int maxNumberOfTypes = 100;
    
    //for the type filter
    /**
     * If an unordered type-pair does not have at least this many triples, it will not have any contexts generated.
     * So if number-number relations never occur, we will never generated contexts for a number-number node-pair.
     */
    public int minTypePairFreq = 1;

    public int minTypeFreqForUnary = 1;
    
    
    public File kbDir() {
        return new File(kbDir);
    }
}
