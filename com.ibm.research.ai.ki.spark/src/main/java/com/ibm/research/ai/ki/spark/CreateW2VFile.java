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

import java.util.*;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.wink.json4j.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

/**
 * From serialized Document, create a space tokenized file, one sentence per line - suitable for word2vec.
 * @author mrglass
 *
 */
public class CreateW2VFile extends SimpleSparkJob {
	private static final long serialVersionUID = 1L;
	
	@Override
	protected JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
		//create the documents
		JavaRDD<Document> docs = documents.map(new Function<String, Document>() {
	            private static final long serialVersionUID = 1L;
	            public Document call(String docStr) throws Exception {
	                return DocumentSerialize.fromString(docStr);
	            }
	        });

		docs.cache();
		
		//pull out the sentences with the terms identified
		JavaRDD<String> sentences = docs.flatMap(new FlatMapFunction<Document,String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Document doc) throws Exception {
				List<String> sents = new ArrayList<>();
				if (doc == null)
					return sents.iterator();
				StringBuilder buf = new StringBuilder();
				for (Sentence s : doc.getAnnotations(Sentence.class)) {
					List<Span> terms = new ArrayList<>();
					terms.addAll(doc.getAnnotations(Token.class, s));
					terms.addAll(doc.getAnnotations(Entity.class, s));
					Collections.sort(terms);
					int prevEnd = 0;
					for (Span t : terms) {
						if (t.start < prevEnd)
							continue;
						prevEnd = t.end;
						buf.append(t.substring(doc.text).trim().replaceAll("\\s+", "_").toLowerCase()).append(' ');
					}
					sents.add(buf.toString());
					buf.setLength(0);
				}
				return sents.iterator();
			}
			
		});
		return sentences;
	}

	   /**
     * Example args 
        -in /data/someBigCorpus.ser.gz.b64 -out /data/text4w2v.txt

       Then get the hdfs text with
        hdfs dfs -getmerge -nl /data/text4w2v.txt corpus.txt

       Then train with word2vec 
        ./word2vec -train corpus.txt -output wordVectors.bin -size 200 -window 5 -sample 1e-4 -negative 5 -binary 1 -cbow 0 -iter 1 -threads `nproc`
       And convert to embedding format
        java -cp kbp-jar-with-dependencies.jar com.ibm.research.ai.ki.kbp.embeddings.Word2VecConverter wordVectors.bin wordVectors.ef

     * @param args
     */	
    public static void main(String[] args) {
    	Options options = new Options();
		options.addOption("in", true, "");
		options.addOption("out", true, "");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}
	
		String inputPath = cmd.getOptionValue("in");
		String outputPath = cmd.getOptionValue("out");

		CreateW2VFile w2v = new CreateW2VFile();
		w2v.run("W2V normalize", inputPath, outputPath);
	}	
}
