package com.ibm.research.ai.ki.kbp;

import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;
import com.ibm.research.ai.ki.util.parallel.*;

/**
 * Baseline EDL using gazetteer matching
 * @author mrglass
 *
 */
public class GazetteerEDL {
    static Annotator annotator;
    
    protected byte[] gazContent;
    
    public GazetteerEDL(String gazFile) {
        try {
            this.gazContent = Files.readAllBytes(Paths.get(gazFile));
        } catch (Exception e) {throw new Error(e);}
    }
    
    public Document annotate(String serDoc) {
        initAnnotator();
        Document doc = DocumentSerialize.fromString(serDoc);
        annotator.process(doc);
        return doc;
    }
    
    static class PostGazClean implements Annotator {
        private static final long serialVersionUID = 1L;

        @Override
        public void initialize(Properties config) {}
        
        static final Span emptySpan = new Span(0,0);
        
        @Override
        public void process(Document doc) {
            List<Token> tokens = doc.getAnnotations(Token.class);
            List<EntityWithId> toRemove = new ArrayList<>();
            for (EntityWithId e : doc.getAnnotations(EntityWithId.class)) {
                List<EntityWithId> subents = doc.getAnnotations(EntityWithId.class, e);
                if (!subents.isEmpty()) {
                    Span tspan = Lang.NVL(Span.toSegmentSpan(e, tokens), emptySpan);
                    if (tspan.length() > 1) {
                        for (EntityWithId se : subents) {
                            if (Lang.NVL(Span.toSegmentSpan(se, tokens), emptySpan).length() <= 1) {
                                toRemove.add(se);
                            }
                        }
                    }
                }
            }
            for (EntityWithId r : toRemove)
                doc.removeAnnotation(r);
        }
        
    }
    
    protected void initAnnotator() {
        try {
        synchronized (GazetteerEDL.class) {
            if (annotator == null) {
                ObjectInputStream ois = new RefactoringObjectInputStream(new ByteArrayInputStream(FileUtil.uncompress(gazContent)));
                Collection<GazetteerMatcher.Entry> gaz = (Collection<GazetteerMatcher.Entry>) ois.readObject();
                ois.close();
                
                annotator = new Pipeline(
                        new ResettingAnnotator(), 
                        new RegexParagraph(), new OpenNLPSentence(), new ClearNLPTokenize(), 
                        new GazetteerMatcher(gaz),
                        new PostGazClean());

                //CONSIDER: POS based matching?
                annotator.initialize(new Properties());
            }
        }
        } catch (Exception e) {
            Lang.error(e);
        }
    }
    
    static class MultiFileOut {
        MultiFileOut(File outDir) {
            this.outDir = outDir;
        }
        File outDir;
        PrintStream out = null;
        int partNum = -1;
        int outCount = -1;
        void println(String sd) {
            if (out == null) {
                ++partNum;
                out = FileUtil.getFilePrintStream(new File(outDir, "part-"+partNum).getAbsolutePath());
                outCount = 0;
            }
            
            ++outCount;
            out.println(sd);
            
            if (outCount > 50000) {
                out.close();
                out = null;
            }
        }
        void close() {
            if (out != null)
                out.close();
        }
    }
    
    /**
     * Non-spark version
     * @param gazetteerFile
     * @param inputDir
     * @param outputFile
     * @param idCountsTsvFile
     */
    public static void process(File gazetteerFile, File inputDir, File outputFile, File idCountsTsvFile) {
        PeriodicChecker report = new PeriodicChecker(100);
        Object2IntOpenHashMap<String> id2count = idCountsTsvFile != null ? new Object2IntOpenHashMap<String>() : null;
        
        DocumentSerialize.Format format = outputFile != null ? DocumentSerialize.formatFromName(outputFile.getName()) : null;
        //PrintStream out = outputFile != null ? FileUtil.getFilePrintStream(outputFile.getAbsolutePath()) : null;
        MultiFileOut out = outputFile != null ? new MultiFileOut(outputFile) : null;
        
        BlockingThreadedExecutor threads = new BlockingThreadedExecutor(5);
        GazetteerEDL edl = new GazetteerEDL(gazetteerFile.getAbsolutePath());
        for (File f : new FileUtil.FileIterable(inputDir)) {
            for (String doc : FileUtil.getLines(f.getAbsolutePath())) {
                if (report.isTime()) {
                    System.out.println("Gazetteer processing on document "+report.checkCount());
                }
                threads.execute(() -> {
                    Document d = edl.annotate(doc);
                    //if idCounts is desired, we build it here
                    if (id2count != null) {
                        synchronized (id2count) {
                            for (EntityWithId e : d.getAnnotations(EntityWithId.class)) {
                                id2count.addTo(e.id, 1);
                            }
                        }
                    }
                    String sd = DocumentSerialize.toString(d, format);
                    if (out != null) {
                        synchronized (out) {
                            out.println(sd);
                        }
                    }
                });
            }
        }
        threads.awaitFinishing();
        if (out != null)
            out.close();
        if (idCountsTsvFile != null) {
            try (PrintStream outIds = FileUtil.getFilePrintStream(idCountsTsvFile.getAbsolutePath())) {
                for (Map.Entry<String, Integer> e : id2count.entrySet()) {
                    outIds.println(e.getKey()+"\t"+e.getValue());
                }
            }
        }
    }
    
    public static void main(String[] args) {
        process(new File(args[0]), new File(args[1]), new File(args[2]), 
                args.length > 3 ? new File(args[3]) : null);
    }
}
