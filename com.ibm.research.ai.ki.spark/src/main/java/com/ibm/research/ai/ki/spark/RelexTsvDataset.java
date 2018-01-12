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
import org.apache.spark.storage.*;
import org.spark_project.guava.collect.*;

import scala.Tuple2;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

/**
 * RelexTsvDataset -> GatherRelexVocab -> RelexTensorDataset
 * 
 * From Document collection with EntityWithId, Sentence and Token, creates the relex tsv dataset.
 * @author mrglass
 *
 */
public class RelexTsvDataset extends SimpleSparkJob {
	private static final long serialVersionUID = 1L;

	protected IGroundTruth gt;
	
	protected final RelexConfig config;
	
	protected final IRelexTsv tsvMaker;
	
	/**
	 * Group id to number of splits (1 if not present)
	 */
	public Object2IntOpenHashMap<String> gsplits;
	
	public RelexTsvDataset(IGroundTruth gt, RelexConfig config, IRelexTsv tsvMaker) {
		this.gt = gt;
		this.config = config;
		this.tsvMaker = tsvMaker;
	}
	
	/**
	 * downsample if desired and post-process the documents
	 * @param docs
	 * @return
	 * @throws Exception
	 */
	protected JavaRDD<Document> preprocess(JavaRDD<Document> docs) throws Exception {
		if (config.documentSampleFraction != 1.0 && config.documentSampleFraction > 0.0) {
			docs = docs.filter(new Function<Document,Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Document doc) throws Exception {
					return doc != null && 
							GroundTruth.getDocumentDownsamplePriority(doc.text) < config.documentSampleFraction;
				}
			});
		}
		
		return docs;
	}
	
	/**
	 * The count for an entity-pair, and whether is is a positive
	 * @author mrglass
	 *
	 */
	public static class CountAndPositive implements Serializable {
		private static final long serialVersionUID = 1L;
		public int count;
		public boolean positive;
	}
	
	/**
	 * Find for each groupId with more than config.maxMentionSet mentions, the number of groups to split it into
	 * @param mentionCount
	 */
	protected void fillGSplits(JavaPairRDD<String,CountAndPositive> mentionCount) {
		gsplits = mentionCount.aggregate(
				new Object2IntOpenHashMap<>(), 
				new Function2<Object2IntOpenHashMap<String>, Tuple2<String,CountAndPositive>, Object2IntOpenHashMap<String>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Object2IntOpenHashMap<String> call(Object2IntOpenHashMap<String> v1, Tuple2<String, CountAndPositive> v2) throws Exception {
						if (v2._2.count > mentionSetsPerGroup * config.maxMentionSet)
							v1.put(v2._1, (int)Math.ceil(v2._2.count / (double)(mentionSetsPerGroup * config.maxMentionSet))); //transform values, dividing through by config.maxMentionSet
						return v1;
					}}, 
				new Function2<Object2IntOpenHashMap<String>, Object2IntOpenHashMap<String>, Object2IntOpenHashMap<String>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Object2IntOpenHashMap<String> call(Object2IntOpenHashMap<String> v1, Object2IntOpenHashMap<String> v2) throws Exception {
						v1.putAll(v2);
						return v1;
					}});		
	}
	
	protected JavaPairRDD<String,IRelexMention> downsample(
			JavaPairRDD<String,IRelexMention> mentions, JavaPairRDD<String,CountAndPositive> mentionCount) 
	{
		//downsample here
		if (config.targetNegativeToPositveRatio > 0) {
			long total = mentionCount.count();
			long positive = mentionCount.filter(new Function<Tuple2<String,CountAndPositive>, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, CountAndPositive> v1) throws Exception {
					return v1._2.positive;
				}
				
			}).count();
			double currentNegativeToPositive = (double)(total-positive)/positive;
			if (currentNegativeToPositive > 1.05*config.targetNegativeToPositveRatio) {
				config.retainNegativeProb = config.negativeExampleSampleFraction * (config.targetNegativeToPositveRatio / currentNegativeToPositive);
				
				JavaPairRDD<String,IRelexMention> mentionsD = mentions.filter(new Function<Tuple2<String,IRelexMention>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, IRelexMention> ms) throws Exception {
						IRelexMention m1 = ms._2;
						return !m1.isNegative() || m1.getNegativeDownsamplePriority() < config.retainNegativeProb;
					}
				});
				mentionsD.cache();
				mentions.unpersist();
				mentions = mentionsD;
			} else {
				config.retainNegativeProb = config.negativeExampleSampleFraction;
			}
		} else {
			config.retainNegativeProb = config.negativeExampleSampleFraction;
		}
		return mentions;
	}
	
	static final int mentionSetsPerGroup = 10;
	
	protected JavaPairRDD<String,List<IRelexMention>> group(JavaPairRDD<String,IRelexMention> mentions) {
	    //FIXME: this is not a clean estimate of the negative to positive ratio
	    //  positives are more likely to be have multiple mentions
		JavaPairRDD<String,CountAndPositive> mentionCount = mentions.aggregateByKey(
				new CountAndPositive(), 
				new Function2<CountAndPositive, IRelexMention, CountAndPositive>() {
					private static final long serialVersionUID = 1L;
					@Override
					public CountAndPositive call(CountAndPositive cp, IRelexMention m) throws Exception {
						cp.positive = !m.isNegative();
						++cp.count;
						return cp;
					}
					
				}, 
				new Function2<CountAndPositive, CountAndPositive, CountAndPositive>() {
					private static final long serialVersionUID = 1L;
					@Override
					public CountAndPositive call(CountAndPositive cp1, CountAndPositive cp2) throws Exception {
						cp1.count += cp2.count;
						return cp1;
					}
					
				});
		mentionCount.persist(StorageLevel.MEMORY_AND_DISK());
		
		if (config.minMentionSet > 1) {
			JavaPairRDD<String,IRelexMention> mentionsD = mentions.subtractByKey(mentionCount.filter(
				new Function<Tuple2<String,CountAndPositive>, Boolean>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(Tuple2<String, CountAndPositive> v1) throws Exception {
						return v1._2.count < config.minMentionSet;
					}
				}));
			mentionsD.cache();
			mentions.unpersist();
			mentions = mentionsD;
		}
		
		mentions = downsample(mentions, mentionCount);
		
		fillGSplits(mentionCount);
		mentionCount.unpersist();
		
		//now change the key to include the split group and drop groups that exceed maxMentionGroups
		if (!gsplits.isEmpty()) {
			JavaPairRDD<String,IRelexMention> mentionsD = mentions.mapToPair(new PairFunction<Tuple2<String,IRelexMention>, String,IRelexMention>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, IRelexMention> call(Tuple2<String, IRelexMention> v1) throws Exception {
					int numSplits = gsplits.getInt(v1._1);
					if (numSplits == 0)
						numSplits = 1;
					IRelexMention m = v1._2;
					int groupSplit = m.groupSplit(numSplits);
					String key = m.groupId()+"\t"+groupSplit;
					if (mentionSetsPerGroup * groupSplit >= 1.5 * config.maxMentionGroups)
						m = null; //indicate we discard this mention
					return Tuple2.apply(key, m);
				}
			});
			mentionsD.cache();
			mentions.unpersist();
			mentions = mentionsD;
		}
		
		//group RelexMentions by id pair + split, and avoid duplicates
		JavaPairRDD<String,List<IRelexMention>> mentionSets = mentions.aggregateByKey(
				new ArrayList<IRelexMention>(), 
				new Function2<List<IRelexMention>, IRelexMention, List<IRelexMention>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public List<IRelexMention> call(List<IRelexMention> ms1, IRelexMention ms2) {
						if (ms2 == null)
							return ms1;
						if (ms1.isEmpty()) {
							return new ArrayList<>(Arrays.asList(ms2));
						}
						return RelexMention.addNoDuplicates(ms1, Arrays.asList(ms2));
					}
				},
				new Function2<List<IRelexMention>, List<IRelexMention>, List<IRelexMention>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public List<IRelexMention> call(List<IRelexMention> ms1, List<IRelexMention> ms2) {
						return RelexMention.addNoDuplicates(ms1, ms2);
					}
				});
		
		//drop empty mention sets (those trimmed by high group id, or other, to-be-added reason)
		int minMentionSet = config.minMentionSet > 0 ? config.minMentionSet : 1;
		mentionSets = mentionSets.filter(new Function<Tuple2<String,List<IRelexMention>>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, List<IRelexMention>> v1) throws Exception {
				return v1._2.size() >= minMentionSet;
			}
		});


		mentionSets.persist(StorageLevel.MEMORY_AND_DISK());
		mentions.unpersist();
		
		return mentionSets;
	}
	
	@Override
	public JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
		//RelexMention to tsv line
	    //TODO: introduce another point where we save intermediate results - maybe after the first map?
		JavaPairRDD<String,List<IRelexMention>> mentionSets = buildMentionSets(documents);		
		JavaRDD<String> tsvMentions = toTsvLines(mentionSets);
		mentionSets.unpersist();
		return tsvMentions;
	}
	
	public JavaPairRDD<String,List<IRelexMention>> buildMentionSets(JavaRDD<String> documents) {
		//create the documents
		JavaRDD<Document> docs = documents.map(new Function<String, Document>() {
			private static final long serialVersionUID = 1L;
			public Document call(String docStr) throws Exception {
			    return DocumentSerialize.fromString(docStr);
			}
		});
		try {
			docs = preprocess(docs);
		} catch (Exception e) {
			Lang.error(e);
		}
		docs.cache();
		//docs.persist(StorageLevel.MEMORY_AND_DISK());
		
		//get the RelexMentions
		JavaPairRDD<String,IRelexMention> mentions = docs.flatMapToPair(
    		new PairFlatMapFunction<Document, String, IRelexMention>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<scala.Tuple2<String, IRelexMention>> call(Document doc) {
                	List<IRelexMention> mentions = tsvMaker.getMentions(doc);
                	return Lists.transform(mentions, m -> scala.Tuple2.apply(m.groupId(), m)).iterator();
                }
    		});
		mentions.cache();
		docs.unpersist();
		//TODO: just go ahead and save here. The rest is just grouping, negative downsampling and stats
		//split off the downsample, group and stats into another class
		
		//group RelexMentions by id pair
		JavaPairRDD<String,List<IRelexMention>> mentionSets = group(mentions);
		
		return mentionSets;
	}
	
	
	
	public static JavaRDD<String> toTsvLines(JavaPairRDD<String,List<IRelexMention>> mentionSets) {
		return mentionSets.values().flatMap(new FlatMapFunction<List<IRelexMention>,String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(List<IRelexMention> ms) throws Exception {
				return Lists.transform(ms, m -> m.toString()).iterator();
			}
        });
	}
	
	/**
	 * 
     * Get the pair RDD of aggregated mention sets
     * @param tsvLines
     * @param mentionClass
     * @param gsplits if non-null, gives the groupId to number of splits. We split groups into multiple mentions to avoid very large mention sets.
	 * @param documentFraction the fraction of the documents to keep
	 * @param filter the filter should apply any new type-pair filters, along with any additional negative downsampling
	 * @return
	 */
	public static <M extends IRelexMention> JavaPairRDD<String,List<M>> getMentionSets(
			JavaRDD<String> tsvLines, Class<M> mentionClass, Object2IntOpenHashMap<String> gsplits, double documentFraction, Function<IRelexMention,Boolean> filter) 
	{    
	    //here we adjust the group splits based on the document fraction (with fewer documents we need to split the mentions into fewer groups)
	    //but note that the mentions that exceeded the maxMentionGroup size have already been discarded
	    if (documentFraction > 0.0 && documentFraction < 1.0 && gsplits != null) {
	        Object2IntOpenHashMap<String> gsplitsAdj = new Object2IntOpenHashMap<>();
	        for (Map.Entry<String, Integer> e : gsplits.entrySet()) {
	            int numSplitsAdj = (int)Math.ceil(e.getValue() * documentFraction);
	            if (numSplitsAdj > 1) {
	                gsplitsAdj.put(e.getKey(), numSplitsAdj);
	            }
	        }
	        gsplits = gsplitsAdj;
	    }
	    
	    final Object2IntOpenHashMap<String> gsplitsF = gsplits;
		JavaPairRDD<String,M> mentions = tsvLines.mapToPair(new PairFunction<String,String,M>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, M> call(String t) throws Exception {
				M m = mentionClass.newInstance();
				m.fromString(t); 
				int numSplits = 1;
				if (gsplitsF != null) {
					numSplits = gsplitsF.getInt(m.groupId());
					if (numSplits == 0)
						numSplits = 1;
				}
				return new Tuple2<String,M>(m.groupId()+"\t"+m.groupSplit(numSplits), m);
			}
		});
		//apply our document or type-pair filters
		if (filter != null || (documentFraction > 0.0 && documentFraction < 1.0)) {    
		    mentions = mentions.filter(new Function<Tuple2<String,M>,Boolean>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Boolean call(Tuple2<String, M> v1) throws Exception {
                    if (filter != null && !filter.call(v1._2))
                        return false;
                    if (documentFraction > 0.0 && documentFraction < 1.0 && !(v1._2.getDocumentLearningCurvePosition() <= documentFraction))
                        return false;
                    return true;
                }  
		    });
		}
		JavaPairRDD<String,List<M>> mentionSets = mentions.aggregateByKey(new ArrayList<>(), 
			new Function2<List<M>, M, List<M>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public List<M> call(List<M> v1, M v2) throws Exception {
					v1.add(v2);
					return v1;
				}
			}, 
			new Function2<List<M>, List<M>, List<M>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public List<M> call(List<M> v1, List<M> v2) throws Exception {
					v1.addAll(v2);
					return v1;
				}
			});
		return mentionSets;
	}
	
    /**
     * Example args 
	-in /blekko/data/docsWithEnt.ser.gz.b64 
	-out /blekko/data/relexMentions.tsv 
	-gt /disks/diskm/mglass/gtOrig.ser.gz 
	-entInGT -gtTypes
     * @param args
     */
    public static void main(String[] args) {
    	Options options = new Options();
		options.addOption("config", true, "");
		options.addOption("gt", true, "");
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

		String groundTruth = cmd.getOptionValue("gt", config.groundTruthFile);
		GroundTruth gt = FileUtil.loadObjectFromFile(groundTruth);
		
		CreateTsvDataset ctd = new CreateTsvDataset(gt, config);
		
		RelexTsvDataset p = new RelexTsvDataset(gt, config, ctd);
		
		p.run("RelexMention tsv from Document collection", inputPath, outputPath);
		
		//and save the updated config
		FileUtil.writeFileAsString(new File(config.convertDir, "finalRelexConfig.properties"), config.toString());
	}	

}
