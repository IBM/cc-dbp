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

import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Gathers counts of the types and entity ids that occur in a document collection.
 * @author mrglass
 *
 */
public class DocEntityStats extends SimpleSparkJob {
	private static final long serialVersionUID = 1L;

	final String saveDir;
	
	public DocEntityStats(String saveDir) {
		this.saveDir = FileUtil.ensureSlash(saveDir);
	}
	
	private static Annotator annotator = null;
	
	protected String gazEntriesFile = null;
	
	protected boolean preTokenized = false;
	
	@Override
	protected JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
		//create the documents
		JavaRDD<Document> docs = documents.map(new Function<String, Document>() {
			private static final long serialVersionUID = 1L;
			public Document call(String text) throws Exception {
				Document doc = DocumentSerialize.fromString(text);
				if (gazEntriesFile != null) {
				    synchronized (DocEntityStats.class) {
				        if (annotator == null) {
				            Collection<GazetteerMatcher.Entry> gazEntries = FileUtil.loadObjectFromFile(gazEntriesFile);
				            annotator = new GazetteerMatcher(gazEntries);
				            if (!preTokenized) {
				                annotator = new Pipeline(
				                        new ResettingAnnotator(), new RegexParagraph(), new OpenNLPSentence(), 
				                        new ClearNLPTokenize(), annotator);
				            }
				            annotator.initialize(new Properties());
				        }
				    }
				    if (preTokenized)
				        doc.removeAnnotations(Entity.class);
				    annotator.process(doc);
				}
				return doc;
			}
		});
		docs.cache();
		
		JavaPairRDD<String,Integer> counts = docs.flatMapToPair(new PairFlatMapFunction<Document,String,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, Integer>> call(Document doc) throws Exception {
				Object2IntOpenHashMap<String> id2count = new Object2IntOpenHashMap<String>();
				for (EntityWithId e : doc.getAnnotations(EntityWithId.class)) {
					if (e.id != null) {
						id2count.addTo("ID:"+e.id, 1);
					}
					if (e.type != null) {
						id2count.addTo("TYPE:"+e.type, 1);
					}
				}
				List<Tuple2<String,Integer>> results = new ArrayList<>();
				for (Object2IntOpenHashMap.Entry<String> e : id2count.object2IntEntrySet()) {
					results.add(Tuple2.apply(e.getKey(), e.getIntValue()));
				}
				return results.iterator();
			}
		});

		counts = counts.reduceByKey(new Function2<Integer,Integer,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//now write the results
		Map<String,Integer> docCounts = counts.collectAsMap();
		PrintStream outTypes = FileUtil.getFilePrintStream(saveDir+"typeCounts.tsv");
		PrintStream outIds = FileUtil.getFilePrintStream(saveDir+"idCounts.tsv");
		for (Map.Entry<String, Integer> e : docCounts.entrySet()) {
			String k = e.getKey();
			if (k.startsWith("ID:")) {
				k = k.substring("ID:".length());
				outIds.println(k+"\t"+e.getValue());
			} else {
				k = k.substring("TYPE:".length());
				outTypes.println(k+"\t"+e.getValue());
			}
		}
		outTypes.close();
		outIds.close();
		
		//also id&type->count
		JavaPairRDD<String,Integer> etCounts = docs.flatMapToPair(new PairFlatMapFunction<Document,String,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, Integer>> call(Document doc) throws Exception {
				Object2IntOpenHashMap<String> id2count = new Object2IntOpenHashMap<String>();
				for (EntityWithId e : doc.getAnnotations(EntityWithId.class)) {
					if (e.id != null && e.type != null) {
						id2count.addTo(e.id+"\t"+e.type, 1);
					}
				}
				List<Tuple2<String,Integer>> results = new ArrayList<>();
				for (Object2IntOpenHashMap.Entry<String> e : id2count.object2IntEntrySet()) {
					results.add(Tuple2.apply(e.getKey(), e.getIntValue()));
				}
				return results.iterator();
			}
		});
		
		Map<String,Integer> entTypes = etCounts.reduceByKey(new Function2<Integer,Integer,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).collectAsMap();
		
		PrintStream outEntTypes = FileUtil.getFilePrintStream(saveDir+"entityTypes.tsv");
		for (Map.Entry<String, Integer> e : entTypes.entrySet()) {
			outEntTypes.println(e.getKey()+"\t"+e.getValue());
		}
		outEntTypes.close();
		
		return null;
	}

	/*
-in /blekko/data/docsWithEnt.ser.gz.b64 -saveDir /disks/diskm/mglass/hypernym/entStats -gazetteer /disks/diskd/gazEntries3.ser.gz

To copy the gazEntries.ser.gz to all workers, use:
/home/mglass/bin/copyAll.sh
	*/
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("saveDir", true, "where to save the tsv count files");	
		options.addOption("in", true, "The input corpus, a document collection with EntityWithId annotations");
		options.addOption("gazetteer", true, "Optional serialized collection of GazetteerMatcher.Entry should be copied to this path on all worker nodes.");
		options.addOption("pretokenized", false, "Indicate if the corpus has already been tokenized (default false)");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}
	
		String inputPath = cmd.getOptionValue("in");
		
		DocEntityStats des = new DocEntityStats(cmd.getOptionValue("saveDir"));
		if (cmd.hasOption("gazetteer"))
		    des.gazEntriesFile = cmd.getOptionValue("gazetteer");
		des.preTokenized = cmd.hasOption("pretokenized");
		des.run("EntityWithId statistics over a document collection.", inputPath, null);
	}
}
