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
package com.ibm.research.ai.ki.kb;

import java.io.*;
import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.nlp.parse.GazetteerMatcher.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Build a gazetteer from the labels.tsv, types.tsv, idCounts.tsv and popularity.tsv.
 * This can be used by GazetteerMatcher to find mentions of the KB entities in text.
 * 
 * @author mrglass
 *
 */
public class BuildGazetteer {
    static class Node {
        Node(String id) {
            if (id.indexOf('\t') != -1 || id.indexOf('\n') != -1)
                throw new IllegalArgumentException("Node IDs must not contain tabs or newlines");
            this.id = id;
        }
        String id;
        double popularity;
        String type = GroundTruth.unknownType;
        boolean caseSensitve;
        int corpusCount;
    }
    
    static Map<String,Node> getId2Node(File kbDir) {
        Map<String,Node> id2node = new HashMap<>();
        if (new File(kbDir, KBFiles.idCountsTsv).exists()) {
            Map<String,MutableDouble> idCounts = SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
            for (Map.Entry<String, MutableDouble> e : idCounts.entrySet()) {
                Node n = new Node(e.getKey());
                n.corpusCount = (int)e.getValue().value;
                id2node.put(e.getKey(), n);
            }
        }
        if (new File(kbDir, KBFiles.typesTsv).exists()) {
            for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
                if (parts.length >= 2)
                    id2node.computeIfAbsent(parts[0], k -> new Node(k)).type = parts[1];
            }
        }
        if (new File(kbDir, KBFiles.popularityTsv).exists()) {
            for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.popularityTsv))) {
                id2node.computeIfAbsent(parts[0], k -> new Node(k)).popularity = Double.parseDouble(parts[1]);
            }
        }
        if (new File(kbDir, "caseSensitive.tsv").exists()) {
            for (String[] parts : new SimpleTsvIterable(new File(kbDir, "caseSensitive.tsv"))) {
                id2node.computeIfAbsent(parts[0], k -> new Node(k)).caseSensitve = true;
            }
        }
        return id2node;
    }
    
    public static void build(File kbDir, int minCount, int maxCount) {
        Map<String,Node> id2node = getId2Node(kbDir);
        
        boolean countFiltered = (new File(kbDir, KBFiles.idCountsTsv).exists());
        
        Annotator tokenizer = new Pipeline(new ClearNLPTokenize()); //CONSIDER: maybe use the digit sequence tokenize?
        tokenizer.initialize(new Properties());
        
        RandomUtil.Sample<String> rejected = new RandomUtil.Sample<>(30);
        Collection<Entry> entries = new ArrayList<>();
        //TODO: we should also do Map<String,Entry>, keeping only the most popular per-label
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.labelsTsv))) {
            String id = parts[0];
            Node n = id2node.get(id);
            int count = countFiltered ? (n == null ? 0 : n.corpusCount) : minCount;
            if (count >= minCount) {
                String type = n == null ? GroundTruth.unknownType : n.type;
                for (int i = 1; i < parts.length; ++i) {
                    String[] tokens = Token.tokenize(tokenizer, parts[i]);
                    //multiword or number or not too frequent
                    if (tokens.length > 0 && (tokens.length > 1 || count <= maxCount || Lang.isInteger(tokens[0])))
                        entries.add(new Entry(id, type, tokens, n != null && n.caseSensitve));
                    else if (rejected.shouldSave())
                        rejected.save(parts[i]+"\t"+id+"\t"+type+"\t"+count);
                }
            }
        }
        FileUtil.saveObjectAsFile(new File(kbDir, "gazEntries"+(countFiltered ? "Filtered" : "")+".ser.gz"), entries);
        if (!rejected.isEmpty())
            System.out.println("Rejects:\n   "+Lang.stringList(rejected, "\n   "));
    }
    
    public static void main(String[] args) {
        //given a kb directory, build a gazetteer
        File kbDir = new File(args[0]);
        int minCount = 1;
        int maxCount = 3000000;
        
        build(kbDir, minCount, maxCount);
    }
}
