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
package com.ibm.research.ai.ki.spark;

import java.io.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public class CorpusStatistics extends SimpleSparkJob {

    private static final long serialVersionUID = 1L;

    public CorpusStatistics(String saveDir) {
        this.saveDir = saveDir;
    }
    protected String saveDir;
    
    public static class CorpusStats implements Serializable {
        private static final long serialVersionUID = 1L;
        long sentenceCount = 0;
        long wordCount = 0;
        long docCount = 1;
        public CorpusStats(Document doc) {
            for (Sentence s : doc.getAnnotations(Sentence.class)) {
                sentenceCount += 1;
            }
            for (Token t : doc.getAnnotations(Token.class)) {
                wordCount += 1;
            }
        }
        public void merge(CorpusStats cs) {
            this.sentenceCount += cs.sentenceCount;
            this.wordCount += cs.wordCount;
            this.docCount += cs.docCount;
        }
        
        public String toString() {
            return "Sentences = "+sentenceCount+"\nWords = "+wordCount+"\nDocuments = "+docCount;
        }
    }
    
    @Override
    protected JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
        CorpusStats stats = documents.map(new Function<String, CorpusStats>() {
            private static final long serialVersionUID = 1L;
            public CorpusStats call(String text) throws Exception {
                Document doc = DocumentSerialize.fromString(text);
                return new CorpusStats(doc);
            }
        }).reduce(new Function2<CorpusStats,CorpusStats,CorpusStats>() {
            private static final long serialVersionUID = 1L;
            @Override
            public CorpusStats call(CorpusStats v1, CorpusStats v2) throws Exception {
                v1.merge(v2);
                return v1;
            }
            
        });
        
        FileUtil.writeFileAsString(new File(saveDir, "corpusStats.txt"), stats.toString());
        
        return null;
    }
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("saveDir", true, "where to save the tsv count files");    
        options.addOption("in", true, "The input corpus, a document collection with EntityWithId annotations");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            Lang.error(pe);
        }
    
        String inputPath = cmd.getOptionValue("in");
        
        CorpusStatistics des = new CorpusStatistics(cmd.getOptionValue("saveDir"));
       
        des.run("Corpus statistics over a document collection.", inputPath, null);
    }
}
