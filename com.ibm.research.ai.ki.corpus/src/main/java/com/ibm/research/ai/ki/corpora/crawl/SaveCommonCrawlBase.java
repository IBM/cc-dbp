/**
 * cc-dbp-dataset
 *
 * Copyright (c) 2017 IBM
 *
 * The author licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.research.ai.ki.corpora.crawl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.zip.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil;

import org.apache.commons.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.jwat.common.*;
import org.jwat.warc.*;

import com.google.common.collect.*;


/**
 * See the common-crawl blog for updates and new crawls: http://commoncrawl.org/connect/blog/
 * For example:
 * http://commoncrawl.org/2017/07/july-2017-crawl-archive-now-available/
 * 
 * See related work at:
    http://matpalm.com/blog/2011/12/10/common_crawl_visible_text/
    https://github.com/matpalm/common-crawl/
    or
    http://digitalpebble.blogspot.com/2012/09/using-behemoth-on-commoncrawl-dataset.html

 * @author mrglass
 *
 */
public abstract class SaveCommonCrawlBase {
    //options about what annotation types to retain
    // possible options are LinkAnnotation, SectionHeader, Paragraph and TextFormating
    protected Set<Class<? extends Annotation>> annotationTypesOfInterest;
    
    
    //Using: https://github.com/optimaize/language-detector
    protected LanguageScorer ldect;
    protected CommonCrawlConfig config;
    //Using forked boilerpipe with link annotations
    protected HtmlToDocument htmlConvert;
    protected DocumentSerialize.Format saveFormat = DocumentSerialize.Format.json;
    protected String warcDir; //local FS directory for warcs
    protected PeriodicChecker report = new PeriodicChecker(100);
    protected AtomicInteger docCount = new AtomicInteger(0);
    
    protected abstract PrintStream getThreadOutput(String baseDir, int threadNum);
    
    public SaveCommonCrawlBase(CommonCrawlConfig config) {
        this.config = config;
        ldect = new LanguageScorer(config.language);
        htmlConvert = new HtmlToDocument();
        annotationTypesOfInterest = new HashSet<>();
        if (config.annotationTypes != null) {
            for (String at : config.annotationTypes) {
                Class<? extends Annotation> ac = DocumentSerialize.annotationClassName(at);
                if (ac == null) {
                    System.err.println("WARNING: Couldn't find annotation type of "+at);
                } else {
                    annotationTypesOfInterest.add(ac);
                }
            }
        }
    }
    
    static boolean isResponse(WarcRecord record) {
        HeaderLine typeHeader = record.getHeader("WARC-Type");
        if (typeHeader != null && typeHeader.value.equals("response")) {
            return true;
        }
        //System.err.println(typeHeader.value);
        return false;
    }
    
    Map<String,MutableDouble> exceptionCount = new HashMap<>();
    List<String> failedFiles = new ArrayList<>();
    
    public List<Document> getDocuments(String sourceFile, String url, int tryCount) {
        if (tryCount > 0) {
            try { Thread.sleep(2000); } catch (Throwable t) {}
        }
        List<Document> docs = new ArrayList<>();
        URLConnection conn = null;
        try {
            conn = new URL(url).openConnection();
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
        } catch (Exception e) {
            if (tryCount < 3) {
                return getDocuments(sourceFile, url, tryCount+1);
            } else {
                failedFiles.add(sourceFile);
                SparseVectors.increase(exceptionCount, "FAILURE:"+e.getMessage(), 1);
                return docs;
            }
        }
        
        try (InputStream is = conn.getInputStream()) {
            
            InputStream stream = is;
            if (url.endsWith("gz"))
                stream = new GZIPInputStream(stream);
            org.jwat.warc.WarcReader reader = WarcReaderFactory.getReader(stream);
            Iterator<WarcRecord> rit = reader.iterator();
            while (rit.hasNext()) {
                try {
                    WarcRecord wr = rit.next();
                    if (!isResponse(wr))
                        continue;
                    byte[] content = IOUtils.toByteArray(wr.getPayloadContent());
                    String str = null;
                    try {
                        str = new String(content, CharsetDetect.getCharsetFromBytes(content));
                    } catch (java.io.UnsupportedEncodingException uee) {} //that's fine
                    if (str == null || str.isEmpty())
                        continue;
                    Document doc = null;
                    String uri = wr.getHeader("WARC-Target-URI").value;
                    //CONSIDER: maybe the id should be sourceFile + wr.getStartOffset()
                    // and the document source should be the url?
                    try {
                        doc = htmlConvert.toDocument(uri, str);
                        doc.removeAnnotations(a -> !annotationTypesOfInterest.contains(a.getClass()));
                        doc.setDocumentStructure(new DocumentSource(sourceFile));
                    } catch (Exception e) {
                        synchronized (exceptionCount) {
                            SparseVectors.increase(exceptionCount, "HTMLConvert:"+e.getMessage(), 1);
                        }
                    } //couldn't parse the html - forget it. (rare anyway)
                    if (doc == null)
                        continue;
                    double score = ldect.score(doc.text);
                    if (score >= config.minLanguageConfidence) {
                        docs.add(doc);
                    }
                } catch (java.net.SocketException | javax.net.ssl.SSLException | java.net.SocketTimeoutException se) {
                    synchronized (exceptionCount) {
                        SparseVectors.increase(exceptionCount, "WARCLoop:"+se.getMessage(), 1);
                    }
                    if (tryCount < 3) {
                        return getDocuments(sourceFile, url, tryCount+1);
                    } else {
                        failedFiles.add(sourceFile);
                        SparseVectors.increase(exceptionCount, "FAILURE:"+se.getMessage(), 1);
                    }
                    System.err.println("On file "+sourceFile);
                    se.printStackTrace();
                } catch (Throwable t) {
                    synchronized (exceptionCount) {
                        SparseVectors.increase(exceptionCount, "WARCLoop:"+t.getMessage(), 1);
                    }
                    System.err.println("On file "+sourceFile);
                    t.printStackTrace();
                }
            }
        } catch (java.net.SocketException | javax.net.ssl.SSLException | java.net.SocketTimeoutException se) {
            synchronized (exceptionCount) {
                SparseVectors.increase(exceptionCount, "URLRead:"+se.getMessage(), 1);
            }
            if (tryCount < 3) {
                return getDocuments(sourceFile, url, tryCount+1);
            } else {
                failedFiles.add(sourceFile);
                SparseVectors.increase(exceptionCount, "FAILURE:"+se.getMessage(), 1);
            }
            System.err.println("On file "+sourceFile);
            se.printStackTrace();
        } catch (Throwable t) {
            //keep track of how many errors of each type
            synchronized (exceptionCount) {
                SparseVectors.increase(exceptionCount, "URLRead: "+t.getClass().getName()+":"+t.getMessage(), 1);
            }
            t.printStackTrace();
        }
        docCount.addAndGet(docs.size());
        
        return docs;
    }
    
    String serialize(Document doc) {
        return DocumentSerialize.toString(doc, saveFormat);
    }
    
    class CrawlWriter extends Thread {
        PrintStream out;
        Collection<String> urls;
        int threadId;
        CrawlWriter(int threadId, PrintStream out, Collection<String> urls) {
            this.threadId = threadId;
            this.out = out;
            this.urls = urls;
        }
        public void run() {
            //CONSIDER: open for appending?
            try (PrintStream processedFiles = FileUtil.getFilePrintStream(new File(warcDir, "finished-"+threadId+".txt").getAbsolutePath())) {
                for (String url : urls) {
                    String sourceFile = url;
                    if (FileUtil.exists(warcDir + url.substring(url.lastIndexOf('/')+1))) {
                        url = "file://" + warcDir + url.substring(url.lastIndexOf('/')+1);
                    } else {
                        url = config.urlPrefix + url;
                    }
                    if (report.isTime()) {
                        System.out.println("On "+url+" #"+report.checkCount()); 
                        //CONSIDER: show exceptionCount once in a while
                        //synchronized (exceptionCount) {
                        //    System.out.println(SparseVectors.toString(exceptionCount));
                        //}
                        System.out.flush();
                    }
                    for (Document doc : getDocuments(sourceFile, url, 0)) {
                        out.println(serialize(doc));
                    }
                    processedFiles.println(sourceFile); processedFiles.flush();
                }
                out.close();
            }
        }
    }
    
    public void writeCrawl(Iterable<String> warcList, String targetDir) {
        try {
            FileUtil.ensureWriteable(new File(warcDir, "x"));
            if (config.warcFileLimit > 0) {
                System.out.println("Limiting to first "+config.warcFileLimit+" WARC files.");
                warcList = Iterables.limit(warcList, config.warcFileLimit);
            }
            List<String>[] urls = CollectionUtil.partition(warcList, config.numThreads);
            System.out.println(urls[0].size()+" urls per thread. "+(urls[0].size()*config.numThreads)+" in total.");
            List<CrawlWriter> writers = new ArrayList<>();
            for (int i = 0; i < urls.length; ++i) {
                if (urls[i].isEmpty())
                    continue;
                PrintStream out = this.getThreadOutput(targetDir, i);
                CrawlWriter cw = new CrawlWriter(i, out, urls[i]);
                cw.setDaemon(true);
                cw.start();
                writers.add(cw);
            }
            for (CrawlWriter cw : writers)
                cw.join();      
        } catch (Exception e) {
            Lang.error(e);
        }
        if (!exceptionCount.isEmpty()) {
            System.err.println("Exception counts:\n");
            System.err.println(SparseVectors.toString(exceptionCount));
            FileUtil.writeFileAsString(new File(this.warcDir, "exceptionCounts.txt"), SparseVectors.toString(exceptionCount));
        }
        if (!failedFiles.isEmpty()) {
            System.err.println("\n\nFailed to download:\n"+Lang.stringList(failedFiles, "\n"));
            FileUtil.writeFileAsString(new File(this.warcDir, "failedFiles.txt"), Lang.stringList(failedFiles, "\n"));
        }
        System.err.flush();
        System.out.println("Created "+docCount.get()+" documents");
    } 
}
