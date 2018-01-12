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

import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.*;
import scala.Boolean;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

/**
 * 
 * @author mrglass
 *
 */
public class GatherRelexVocab extends SimpleSparkJob {
	private static final long serialVersionUID = 1L;

	protected File saveDir;
	
	protected final RelexConfig config;
	
	protected final Class<? extends IRelexMention> mentionClass;
	
	public GatherRelexVocab(RelexConfig config, Class<? extends IRelexMention> mentionClass) {
		this.saveDir = new File(config.convertDir);
		this.config = config;
		this.mentionClass = mentionClass;
	}
	
	@Override
	public JavaRDD<String> process(JavaRDD<String> tsvLines) throws Exception {
		JavaRDD<RelexVocab> vocabs = tsvLines.map(new Function<String,RelexVocab>() {
			private static final long serialVersionUID = 1L;
			@Override
			public RelexVocab call(String v1) throws Exception {
				IRelexMention m = mentionClass.newInstance();
				m.fromString(v1);
				return new RelexVocab(m, Tokenizer.getTokenizer(config));
			}
		});

		//gather the types and relations: vocabs.aggregate(zeroValue, seqOp, combOp)
		Set<String> types = vocabs.aggregate(new HashSet<String>(), 
			new Function2<Set<String>, RelexVocab, Set<String>>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Set<String> call(Set<String> types, RelexVocab vocab) throws Exception {
					types.addAll(vocab.getTypes());
					return types;
				}
			}, 
			new Function2<Set<String>, Set<String>, Set<String>>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Set<String> call(Set<String> types1, Set<String> types2) throws Exception {
					types1.addAll(types2);
					return types1;
				}
			});
		//save types (sorted)
		config.entityTypes = RelexVocab.toArray(types);
		//RelexVocab.saveToFile(new File(saveDir, RelexDatasetFiles.type2id), types);
		
		Set<String> relations = vocabs.aggregate(new HashSet<String>(), 
				new Function2<Set<String>, RelexVocab, Set<String>>() {
					private static final long serialVersionUID = 1L;
		
					@Override
					public Set<String> call(Set<String> rels, RelexVocab vocab) throws Exception {
						rels.addAll(vocab.getRelations());
						return rels;
					}
				}, 
				new Function2<Set<String>, Set<String>, Set<String>>() {
					private static final long serialVersionUID = 1L;
		
					@Override
					public Set<String> call(Set<String> rels1, Set<String> rels2) throws Exception {
						rels1.addAll(rels2);
						return rels1;
					}
				});
		//save relations (sorted)
		config.relationTypes = RelexVocab.toArray(relations);
		//RelexVocab.saveToFile(new File(saveDir, RelexDatasetFiles.relation2id), relations);
		
		JavaPairRDD<String,Integer> wordCounts = vocabs.flatMapToPair(new PairFlatMapFunction<RelexVocab, String,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, Integer>> call(RelexVocab t) throws Exception {
				//is this actually serializable though?
				List<Tuple2<String,Integer>> wc = new ArrayList<>();
				for (Entry<String> e : t.getWordCounts()) {
					wc.add(new Tuple2<String, Integer>(e.getKey(), e.getIntValue()));
				}
				return wc.iterator();
			}	
		});
		
		wordCounts = wordCounts.reduceByKey(new Function2<Integer,Integer,Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer c1, Integer c2) throws Exception {
				return c1 + c2;
			}
		});
		
		
		//trim to maxVocabSize with minWordCount
		
		//2, 3, 5, 8, 12, 18, 27, 41, 62, 93
		int lowCountBuckets = 10;
		if (config.vocabMinCount < 2)
			config.vocabMinCount = 2;
		int countThreshold = config.vocabMinCount;
		int[] countThresholds = new int[lowCountBuckets];
		for (int i = 0; i < countThresholds.length; ++i) {
			countThresholds[i] = countThreshold;
			countThreshold += Math.ceil(0.5 * countThreshold);
		}
		//find how many words have less than countThreshold[i] counts
		int[] lowCountCounts = wordCounts.aggregate(new int[countThresholds.length+1], 
				new Function2<int[], Tuple2<String,Integer>, int[]>() {
					private static final long serialVersionUID = 1L;

					@Override
					public int[] call(int[] countCounts, Tuple2<String, Integer> wordCount) throws Exception {
						int c = wordCount._2;
						for (int i = 0; i < countThresholds.length; ++i) {
							if (c < countThresholds[i]) {
								++countCounts[i];
								break;
							}
						}
						countCounts[countCounts.length-1] += 1; //final bucket is total word count
						return countCounts;
					}
				}, 
				new Function2<int[], int[], int[]>() {
					private static final long serialVersionUID = 1L;

					@Override
					public int[] call(int[] countCounts1, int[] countCounts2) throws Exception {
						for (int i = 0; i < countCounts1.length; ++i) {
							countCounts1[i] += countCounts2[i];
						}
						return countCounts1;
					}
				});
		//we'll set the config.vocabMinCount to make sure we get config.vocabLimit
		int totalCount = lowCountCounts[lowCountCounts.length-1];
		int countThreshNdx = 0;
		do {
			totalCount -= lowCountCounts[countThreshNdx];
			config.vocabMinCount = countThresholds[countThreshNdx];
			++countThreshNdx;
		} while (totalCount > config.vocabLimit && countThreshNdx < countThresholds.length);
		System.out.println("Filtering "+config.vocabMinCount+" to get "+totalCount+" words, limit "+config.vocabLimit);
		config.vocabLimit = totalCount;
		
		//filter out the words with counts below config.vocabMinCount
		wordCounts = wordCounts.filter(new Function<Tuple2<String,Integer>,java.lang.Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public java.lang.Boolean call(Tuple2<String, Integer> wc) throws Exception {
				return wc._2 >= config.vocabMinCount;
			}
		});
		
		/*
		//trim to maxVocabSize with minWordCount
		long wordCountSize;
		boolean first = true;
		do {
			if (!first) {
				//increase minCount by 50% each time
				config.vocabMinCount += Math.ceil(0.5 * config.vocabMinCount);
			}
			wordCounts = wordCounts.filter(new Function<Tuple2<String,Integer>,java.lang.Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public java.lang.Boolean call(Tuple2<String, Integer> v1) throws Exception {
					return v1._2 >= config.vocabMinCount;
				}
			});
			wordCountSize = wordCounts.count();
			first = false;
		} while (wordCountSize > config.vocabLimit);
		config.vocabLimit = (int)wordCountSize;
		*/
		
		if (config.initialEmbeddingsFile != null) {
			List<String> words = wordCounts.keys().collect();
			System.out.println("Vocab size = "+words.size());
			RelexVocab.writeVocab(
					new File(config.initialEmbeddingsFile), 
					new File(saveDir, RelexDatasetFiles.wordVectors), 
					words, new String[0]);
		}
		
		if (config.saveVocabRDD) {
			return wordCounts.map(new Function<Tuple2<String,Integer>,String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String call(Tuple2<String, Integer> wc) throws Exception {
					return wc._1.replace('\t', ' ').replace('\n', ' ') + "\t" + wc._2;
				}
				
			});
		} else {
			return null;
		}
	}

    /**
     * Example args

-in /blekko/data/relexMentions.tsv -config /disks/diskm/mglass/relexConfig.properties

/disks/diskm/mglass/blekkow2v/wi-new-to-20160817-w2v-norm-dim200-win5.ef
     * @param args
     */
    public static void main(String[] args) {
    	Options options = new Options();
    	options.addOption("config", true, "");
		options.addOption("convertDir", true, "");
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

		String configProperties = cmd.getOptionValue("config");
		RelexConfig config = new RelexConfig();
		config.fromString(FileUtil.readFileAsString(configProperties));
		config.convertDir = cmd.getOptionValue("convertDir", config.convertDir);

		GatherRelexVocab v = new GatherRelexVocab(config, RelexMention.class);
		
		v.run("Vocab from Relex Tsv", inputPath, outputPath);
		FileUtil.writeFileAsString(new File(config.convertDir, "vocabRelexConfig.properties"), config.toString());
	}
}
