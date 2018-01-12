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
package com.ibm.research.ai.ki.kbp;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;

import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Gathers statistics about the dataset
 * @author mrglass
 *
 */
public class RelexStats implements Serializable {
	private static final long serialVersionUID = 1L;

	protected Object2IntOpenHashMap<String> perRelationPositives = new Object2IntOpenHashMap<>();
	protected Int2IntOpenHashMap numRelsCount = new Int2IntOpenHashMap();
	protected Int2IntOpenHashMap mentionCounts = new Int2IntOpenHashMap();
	protected Object2IntOpenHashMap<String> perRelationMentions = new Object2IntOpenHashMap<>();
	protected int mentionsPositive = 0;
	protected int mentionsNegative = 0;
	
	//TODO: this could be done much more efficiently, should have a single copy of the ground truth per machine
	//for each ground truth entity pair, how many mentions do we have for it
	//protected ObjectSet<String> entitySetIdsInGT; //this will be the one object shared for the whole machine
	public Map<String,MutableDouble> coveragePerGTInstance;
	//the same, broken down by relation
	public Map<String, HashMap<String,MutableDouble>> relTypeCoveragePerGTInstance;
	

	//public static ObjectSet<String> entitySetIdsInGT(IGroundTruth gt) { }
	
	//with GroundTruth loaded we can track coverage of the ground truth.
	public RelexStats(IGroundTruth gt) {
		if (gt != null) {
			coveragePerGTInstance = new HashMap<>();
			relTypeCoveragePerGTInstance = new HashMap<>();
			for (Map.Entry<String, String[]> e : gt.buildEntitySetId2Relations().entrySet()) {
				coveragePerGTInstance.put(e.getKey(), new MutableDouble(0.0));
				for (String rel : e.getValue()) {
					SparseVectors.increase(relTypeCoveragePerGTInstance, rel, e.getKey(), 0.0);
				}
			}
		}
	}
	
	static <T> void addTo(Object2IntOpenHashMap<T> dest, Object2IntOpenHashMap<T> inc) {
	    for (Object2IntOpenHashMap.Entry<T> e : inc.object2IntEntrySet()) {
	        dest.addTo(e.getKey(), e.getIntValue());
	    }
	}
	static void addTo(Int2IntOpenHashMap dest, Int2IntOpenHashMap inc) {
        for (Int2IntOpenHashMap.Entry e : inc.int2IntEntrySet()) {
            dest.addTo(e.getIntKey(), e.getIntValue());
        }
    }
	
	public void merge(RelexStats other) {
		if (this.coveragePerGTInstance != null)
			SparseVectors.addTo(this.coveragePerGTInstance, other.coveragePerGTInstance);
		if (this.relTypeCoveragePerGTInstance != null)
			SparseVectors.addTo2(this.relTypeCoveragePerGTInstance, other.relTypeCoveragePerGTInstance);
		
		addTo(perRelationPositives, other.perRelationPositives);
		addTo(numRelsCount, other.numRelsCount);
		addTo(mentionCounts, other.mentionCounts);
		addTo(perRelationMentions, other.perRelationMentions);
		
		this.mentionsPositive += other.mentionsPositive;
		this.mentionsNegative += other.mentionsNegative;
	}
		
	public void noteMention(List<? extends IRelexMention> mentions) {
		// with group splits our mentions are not 'valid' anymore'
		if (!RelexMention.isValidSet(mentions)) {
			throw new IllegalArgumentException("bad mention set: "+
					Lang.stringList(Iterables.transform(mentions, m -> m.groupId()), ";; "));
		}
	
		IRelexMention m = mentions.get(0);
		String[] rels = m.getRelations();
		if (m.isNegative()) {
			mentionsNegative += mentions.size();
		} else {
			mentionsPositive += mentions.size();
		}
		numRelsCount.addTo(rels.length, 1);
		mentionCounts.addTo(mentions.size(), 1);
		for (String rel : rels) {
			perRelationPositives.addTo(rel, 1);
			perRelationMentions.addTo(rel, mentions.size());
		}
		
		if (coveragePerGTInstance != null) {
			String entSet = m.entitySetId();
			MutableDouble v = coveragePerGTInstance.get(entSet);
			if (v != null) {
				v.value += mentions.size();
    			for (String rel : rels) {
    			    rel = GroundTruth.unorderedRelation(rel);
    				//why 1.0? shouldn't it be mentions.size()?
    				if (SparseVectors.increase(relTypeCoveragePerGTInstance, rel, entSet, 1.0))
    					throw new Error("this should not create new entries! added "+entSet+" related by "+rel);
    			}
			}
		}
	}
	
	static int[] getHistogram(Map<Integer,Integer> m, double[] thresholds) {
		int[] histo = new int[thresholds.length+1];
		for (Map.Entry<Integer, Integer> e : m.entrySet()) {
			if (e.getKey() >= thresholds[thresholds.length-1]) {
				histo[thresholds.length] += (int)e.getValue();
			} else {
				for (int i = 0; i < thresholds.length; ++i) {
					if (e.getKey() < thresholds[i]) {
						histo[i] += (int)e.getValue();
						break;
					}
				}
			}
		}
		return histo;
	}
	
	//CONSIDIER: toXls - make a nice excel file with charts
	//  http://stackoverflow.com/questions/34718734/apache-poi-supports-only-scattercharts-and-linecharts-why
	
	static <T> String toString(Object2IntOpenHashMap<T> vec) {
	    StringBuilder buf = new StringBuilder();
	    int maxLen = 0;
	    List<Pair<String,Integer>> results = new ArrayList<>();
	    for (Object2IntOpenHashMap.Entry<T> e : vec.object2IntEntrySet()) {
	        String s = e.getKey().toString();
	        maxLen = Math.max(s.length(), maxLen);
	        results.add(Pair.of(s,e.getIntValue()));
	    }
	    SecondPairComparator.sortR(results);
	    for (Pair<String,Integer> r : results) {
	        buf.append(Lang.LPAD(r.first, maxLen)+" "+r.second).append('\n');
	    }
	    return buf.toString();
	}
	
	
	public String toString() {
		StringBuilder buf = new StringBuilder();
		int positiveCount = perRelationPositives.values().stream().reduce((x,y) -> x+y).get();
		int negativeCount = numRelsCount.get(0);
		buf.append("Positives = "+positiveCount+" Negatives = "+negativeCount+"; "+((double)negativeCount / positiveCount)+" negative-to-positive\n");
		buf.append("Positive Mentions = "+mentionsPositive+" Negative Mentions = "+mentionsNegative+"\n");
		buf.append("Positives per relation:\n"+toString(perRelationPositives));
		buf.append("Mention counts per relation:\n"+toString(perRelationMentions));
		
		double[] mentionCountThresholds = new double[] {2,3,4,8,16,32};
		int[] mentionCountHisto = getHistogram(mentionCounts, mentionCountThresholds);
		buf.append("Mention counts (how many mentions per entity-pair):\n"+Distribution.stringHisto(mentionCountThresholds, mentionCountHisto)+"\n");

		double[] relCountThresholds = new double[] {1,2,3,4,8,16};
		int[] relCountHisto = getHistogram(numRelsCount, relCountThresholds);
		buf.append("Positive Relation count histogram (how multi-label is this task):\n"+Distribution.stringHisto(relCountThresholds, relCountHisto)+"\n");

		if (coveragePerGTInstance != null) {
			double[] gtCoverageThresholds = new double[] {1,2,3,4,8,16};
			int[] gtCoverageHisto = SparseVectors.getHisto(coveragePerGTInstance, gtCoverageThresholds);
			//CONSIDER: sample the id pairs without coverage?
			buf.append("Coverage of ground truth:\n"+Distribution.stringHisto(gtCoverageThresholds, gtCoverageHisto)+"\n");
			buf.append("  percent out of recall = "+((double)gtCoverageHisto[0]/DenseVectors.sum(gtCoverageHisto))+"\n");
			for (Map.Entry<String, HashMap<String,MutableDouble>> e : relTypeCoveragePerGTInstance.entrySet()) {
				gtCoverageHisto = SparseVectors.getHisto(e.getValue(), gtCoverageThresholds);
				buf.append(e.getKey()+" relation type coverage of ground truth:\n"+Distribution.stringHisto(gtCoverageThresholds, gtCoverageHisto)+"\n");
				buf.append("  percent out of recall = "+((double)gtCoverageHisto[0]/DenseVectors.sum(gtCoverageHisto))+"\n");
			}
		}
		
		
		return buf.toString();
	}
	
	/**
	 * Example args:
	 * simpleFormat/train.tsv simpleFormat/test.tsv
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("groundTruth", true, "the serialized GroundTruth object");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}

		List<String> tsvFiles = cmd.getArgList();
		
		GroundTruth gt = null;
		if (cmd.hasOption("groundTruth"))
			gt = FileUtil.loadObjectFromFile(cmd.getOptionValue("groundTruth"));
		RelexStats stats = new RelexStats(gt);
		
		
		for (String fset : tsvFiles) {
			for (File f : new FileUtil.FileIterable(new File(fset))) {
				for (List<RelexMention> m : RelexMentionReader.getSetReader(f, RelexMention.class)) {
					stats.noteMention(m);
				}
			}
		}
		System.out.println(stats.toString());
	}
}
