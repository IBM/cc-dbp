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
package com.ibm.research.ai.ki.kb.conversion;

import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kb.*;
import com.ibm.research.ai.ki.kb.conversion.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;

import com.google.common.collect.*;

/**
 sunburst chart on entity and relation types
  relexStats.txt + relationTaxonomy.tsv -> sunburst relation count
  idCounts.tsv + type heirarchy -> sunburst type count (mention and instance)
  300 relations with at least 5 instance pairs in june crawl
  
 distribution of entity mention counts (log scale) - maybe base2
 
 needs tensorfilereader:
 distribution of context set size
 distribution of number of relations (not counting heirarchical)

 * @author mrglass
 *
 */
public class SummaryCharts {
    
    private static int relationCount(Set<Integer> leafRels, int[] gt) {
        int rcount = 0;
        for (int i = 0; i < gt.length; ++i)
            if (gt[i] > 0 && leafRels.contains(i))
                ++rcount;
        return rcount;
    }
    
    //TODO: provide examples of high relation counts
    private static void datasetDistributions(String relexConfigFile, String kbDir, double datasetFraction, String... dataDirs) {
        RelexConfig rconf = new RelexConfig();
        rconf.fromString(FileUtil.readFileAsString(relexConfigFile));
        String[] relationTypes = rconf.relationTypes;
        
        //load relation taxonomy
        Set<String> nonLeaf = new HashSet<>();
        for (String[] parts : new SimpleTsvIterable(new File(kbDir,"relationTaxonomy.tsv"))) {
            nonLeaf.add(parts[1]);
        }
        
        Set<Integer> leafRels = new HashSet<>();
        for (int i = 0; i < relationTypes.length; ++i)
            if (!nonLeaf.contains(relationTypes[i].substring(1)))
                leafRels.add(i);
        
        
        Map<String,MutableDouble> id2contexts = new HashMap<>();
        int[] rcounts = new int[30];
        RandomUtil.Sample<String>[] highRelSamples = new RandomUtil.Sample[rcounts.length-2];
        for (int i = 0; i < highRelSamples.length; ++i)
            highRelSamples[i] = new RandomUtil.Sample<String>(10);
        PeriodicChecker report = new PeriodicChecker(100);
        for (String tensorDir : dataDirs) {
            for (Object[] ts : new TensorFileReader(new File(tensorDir), datasetFraction)) {
                if (report.isTime()) {
                    System.out.println("On tensor "+report.checkCount());
                }
                String id = (String)ts[0];
                //number of contexts
                int[] sentStarts = (int[])ts[3];
                SparseVectors.increase(id2contexts, id, sentStarts.length);
                
                
                int[] gt = (int[])ts[ts.length-1];
                int rcount = relationCount(leafRels, gt);
                if (rcount >= rcounts.length) {
                    System.err.println("WARNING: out of limit: "+rcount);
                } else {
                    ++rcounts[rcount];
                    if (rcount > 1) {
                        if (highRelSamples[rcount-2].shouldSave()) {
                            StringBuilder buf = new StringBuilder();
                            buf.append(id);
                            for (int ri = 0; ri < gt.length; ++ri) {
                                if (gt[ri] > 0 && leafRels.contains(ri)) {
                                    String rel = relationTypes[ri];
                                    buf.append(", ").append(rel);
                                }
                            }
                            highRelSamples[rcount-2].save(buf.toString());
                        }
                    }
                }
            }
        }
        System.out.println("Multi-labelness");
        for (int i = 0; i < rcounts.length; ++i) {
            System.out.println(i+"\t"+rcounts[i]);
        }
        System.out.println("Multi-label examples");
        for (int i = 2; i < rcounts.length; ++i) {
            if (highRelSamples[i-2].isEmpty())
                continue;
            System.out.println(i+"\n   "+Lang.stringList(highRelSamples[i-2], "\n   "));
        }
        
        Map<Integer,MutableDouble> contextDist = new HashMap<>();
        int[] countCounts = new int[28];
        int outOfCount = 0;
        Random rand = new Random(123);
        for (MutableDouble v : id2contexts.values()) {
            double lv = Math.log(v.value) / Math.log(2);
            int bucket = (int)Math.floor(lv) + (rand.nextDouble() < lv - Math.floor(lv) ? 1 : 0);
            if (bucket >= countCounts.length) {
                ++outOfCount;
                bucket = countCounts.length-1;
            }
            countCounts[bucket]++;
        }
        System.out.println("Context dist");
        for (int i = 0; i < countCounts.length; ++i) {
            int c = (int)Math.pow(2, i);
            System.out.println(c +"\t"+ countCounts[i]);
        }
        System.out.println("Out of count = "+outOfCount);
        /*
        for (MutableDouble contexts : id2contexts.values()) {
            SparseVectors.increase(contextDist, (int)contexts.value, 1.0);
        }
        System.out.println("Context count");
        System.out.println(SparseVectors.toString(contextDist, 100));
        */
    }
    
    private static void entityMentionCountDistribution(String kbDir) {
        Map<String,MutableDouble> idCounts = 
                SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
        int[] countCounts = new int[28];
        int outOfCount = 0;
        Random rand = new Random(123);
        for (MutableDouble v : idCounts.values()) {
            double lv = Math.log(v.value) / Math.log(2);
            int bucket = (int)Math.floor(lv) + (rand.nextDouble() < lv - Math.floor(lv) ? 1 : 0);
            if (bucket >= countCounts.length) {
                ++outOfCount;
                bucket = countCounts.length-1;
            }
            countCounts[bucket]++;
        }
        for (int i = 0; i < countCounts.length; ++i) {
            int c = (int)Math.pow(2, i);
            System.out.println(c +"\t"+ countCounts[i]);
        }
        System.out.println("Out of count = "+outOfCount);
    }
    
    private static final boolean yearHeuristic = false;
    
    private static void entityTypeSunburst(String dbpediaDir, String kbDir, int maxDepth, boolean includeLiterals) {
        Map<String,Set<String>> type2directSubtype = ConvertDBpedia.entityTypeHierarchy(
                new File(dbpediaDir, "dbpedia_2016-10.owl").getAbsolutePath());
        Map<String,MutableDouble> idCounts = 
                SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
        Object2IntOpenHashMap<String> type2count = new Object2IntOpenHashMap<String>();
        for (String[] t : NTriples.getProperties(new File(dbpediaDir, "instance_types_transitive_en.ttl.bz2"))) {
            String id = t[0];
            id = ConvertDBpedia.useNamespacePrefix(id);
            if (idCounts.containsKey(id)) {
                String type = ConvertDBpedia.useNamespacePrefix(t[2]);
                if (type.startsWith("dbo:") && !type.contains("Wikidata"))
                    type2count.addTo(type, 1);
            }
        }
        if (includeLiterals) {
            for (String id : idCounts.keySet()) {
                if (id.startsWith("dbl:")) {
                    String lit = id.substring(4);
                    if (Lang.isDouble(lit)) {
                        if (yearHeuristic && Lang.isInteger(lit) && Integer.parseInt(lit) > 1500 && Integer.parseInt(lit) <= 2018)
                            type2count.addTo("dbl:year", 1);
                        else
                            type2count.addTo("dbl:number", 1);
                    } else {
                        type2count.addTo("dbl:string", 1);
                    }
                    type2count.addTo("dbl:literal", 1);
                }
            }
            type2directSubtype.put("dbl:literal", new HashSet<>(Arrays.asList("dbl:number", "dbl:string", "dbl:year")));
        }
        
        List<String> lines = sunburst(type2directSubtype, type2count, maxDepth);

        System.out.println(Lang.stringList(lines, "\n"));
    }
    
    private static List<String> sunburst(Map<String,Set<String>> type2directSubtype, Map<String,Integer> type2count, int maxDepth) {
        Map<String,Integer> type2directCount = new HashMap<>();
        for (Map.Entry<String, Integer> ec : type2count.entrySet()) {
            int totalCount = ec.getValue();
            Set<String> subs = type2directSubtype.get(ec.getKey());
            if (subs != null) {
                for (String sub : subs) {
                    totalCount -= Lang.NVL(type2count.get(sub), 0);
                }
            }
            type2directCount.put(ec.getKey(), totalCount);
        }
        
        //select the 'top level' types
        Set<String> topTypes = new HashSet<>(type2directCount.keySet());
        for (Map.Entry<String, Set<String>> e : type2directSubtype.entrySet()) {
            if (type2directCount.containsKey(e.getKey())) {
                topTypes.removeAll(e.getValue());
            }
        }
        
        List<String> lines = new ArrayList<>();
        for (String tt : topTypes) {
            addSunburstLines("", tt, maxDepth, 
                    type2directCount, type2directSubtype, type2count,
                    lines);
        }
        return lines;
    }
    
    //FIXME: but remember we changed *date to *year, so we need to check those in the taxonomy
    private static void relationTypeSunburst(String dbpediaDir, int maxDepth) {
        //relexStats.txt + relationTaxonomy.tsv -> sunburst relation count
        Map<String,MutableDouble> relCountsD = 
                SparseVectors.fromString(FileUtil.readFileAsString(new File(dbpediaDir, "relationCounts.txt")));
        Map<String,Integer> relCounts = Maps.transformValues(relCountsD, v -> (int)v.value);
        
        Map<String,Set<String>> type2directSubtype = ConvertDBpedia.relation2DirectSubRelation(
                new File(dbpediaDir, "dbpedia_2016-10.owl").getAbsolutePath());
        
        List<String> lines = sunburst(type2directSubtype, relCounts, maxDepth);

        System.out.println(Lang.stringList(lines, "\n"));
    }
    
    private static void addSunburstLines(String supTypes, String type, int depthLimit, 
            Map<String,Integer> type2directCount, Map<String,Set<String>> type2directSubtype, Map<String,Integer> type2count,
            Collection<String> lines) {
        Integer dcount = type2directCount.get(type);
        if (dcount == null)
            return;
        
        Set<String> subs = type2directSubtype.get(type);
        if (depthLimit == 0 || subs == null || subs.isEmpty()) {
            //add everything from lower levels to dcount
            dcount = type2count.get(type);
        }
        
        //chop the prefix stuff
        type = type.substring(1+Math.max(type.lastIndexOf(':'), type.lastIndexOf('/')));
        
        if (dcount > 0) {
            lines.add(supTypes+type+"\t"+Lang.LPAD("", '\t', depthLimit)+dcount);
        }
        if (depthLimit == 0 || subs == null || subs.isEmpty()) {
            return;
        }
        
        for (String sub : subs) {
            addSunburstLines(supTypes+type+"\t", sub, depthLimit-1,
                    type2directCount, type2directSubtype, type2count,
                    lines);
        }
    }
    
    private static void statsFromTsv(File... tsvFiles) {
        Map<String,MutableDouble> relTypeCount = new HashMap<>();
        Map<String,HashSet<String>> entTypeCount = new HashMap<>();
        int[] countCounts = new int[28];
        int outOfCount = 0;
        Random rand = new Random(123);
        for (File tsvFile : tsvFiles) {
            for (List<RelexMention> ms : new RelexMentionReader.SetReader<RelexMention>(new RelexMentionReader<RelexMention>(tsvFile, RelexMention.class))) {
                RelexMention m = ms.get(0);
                for (String rt : m.relTypes)
                    SparseVectors.increase(relTypeCount, rt, 1.0);
                HashMapUtil.addHS(entTypeCount, m.type1, m.id1);
                HashMapUtil.addHS(entTypeCount, m.type2, m.id2);
                
                double lv = Math.log(ms.size()) / Math.log(2);
                int bucket = (int)Math.floor(lv) + (rand.nextDouble() < lv - Math.floor(lv) ? 1 : 0);
                if (bucket >= countCounts.length) {
                    ++outOfCount;
                    bucket = countCounts.length-1;
                }
                countCounts[bucket]++;
            }
        }
        System.out.println("Relation\tCount");
        for (Map.Entry<String, MutableDouble> e : relTypeCount.entrySet()) {
            System.out.println(e.getKey()+"\t"+((int)e.getValue().value));
        }
        System.out.println("\n\nType\tCount");
        for (Map.Entry<String, HashSet<String>> e : entTypeCount.entrySet()) {
            System.out.println(e.getKey()+"\t"+e.getValue().size());
        }
        
        System.out.println("\n\nContext Set Sizes");
        for (int i = 0; i < countCounts.length; ++i) {
            int c = (int)Math.pow(2, i);
            System.out.println(c +"\t"+ countCounts[i]);
        }
        System.out.println("Out of count = "+outOfCount);
    }
    
    public static void main(String[] args) {
        //entityTypeSunburst(args[0], args[1], 2, true);
        //relationTypeSunburst(args[0], 1);
        //entityMentionCountDistribution(args[1]);
        
/*
        datasetDistributions("where/ever/finalRelexConfig0.properties", 
                args[1], 
                1.0,
                "where/ever/validateDir");
       */
    }
}
