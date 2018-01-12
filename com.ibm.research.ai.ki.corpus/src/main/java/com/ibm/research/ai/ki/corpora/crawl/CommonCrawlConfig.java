package com.ibm.research.ai.ki.corpora.crawl;

import com.ibm.research.ai.ki.util.*;

public class CommonCrawlConfig extends PropertyStruct {
    private static final long serialVersionUID = 1L;

    /**
     * See https://github.com/optimaize/language-detector for langauge options
     */
    public String language = "en";
    /**
     * The language detector is typically very confident, most values are close to one or zero
     */
    public double minLanguageConfidence = 0.8;
    /**
     * Possible options are LinkAnnotation, SectionHeader, Paragraph and TextFormating.
     * LinkAnnotation retains the anchor tag information (which spans of text are links and where they link to).
     */
    public String[] annotationTypes = new String[] {"LinkAnnotation"};
    /**
     * Number of threads downloading parts of Common Crawl, also the number of part files that will be created.
     */
    public int numThreads = 8;
    /**
     * URL prefix to add to the WARC file list
     */
    public String urlPrefix = "https://commoncrawl.s3.amazonaws.com/";
    
    /**
     * To download only a portion of common crawl, limited to this many files.
     */
    public int warcFileLimit;
}
