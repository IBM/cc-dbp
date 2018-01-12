package com.ibm.research.ai.ki.kb.conversion;

import java.io.*;

import com.ibm.research.ai.ki.kb.*;

public class DBpediaKBConfig extends KBConfig {
    private static final long serialVersionUID = 1L;
    
    public String dbpediaOwlUrl;
    
    public String objectsUrl;
    
    public String literalsUrl;
    
    public String labelsUrl;
    
    public String typesUrl;
    
    /**
     * We can construct the KB without using idCounts.tsv if desired. Since getting idCounts.tsv requires running a gazetteer over the corpus and is potentially slow.
     */
    public boolean noNodeCorpusCounts;
    
    
    protected File file(String url) {
        return new File(kbDir, url.substring(url.lastIndexOf('/')+1));
    }
    
    public File dbpediaOwlFile() {
        return file(dbpediaOwlUrl);
    }
    
    public File objectsFile() {
        return file(objectsUrl);
    }
    
    public File literalsFile() {
        return file(literalsUrl);
    }
    
    public File labelsFile() {
        return file(labelsUrl);
    }
    
    public File typesFile() {
        return file(typesUrl);
    }
}
