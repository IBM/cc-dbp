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
package com.ibm.research.ai.ki.kb.explore;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kb.*;
import com.ibm.research.ai.ki.kbp.unary.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.RandomUtil.*;

/**
 * This utility takes a kbDir as input and displays cases of two nodes with the same label (case sensitive and insensitive).
 * The point is to determine a strategy to decide when to merge nodes, when to drop one of the nodes, how to prioritize gazetteer matching.
 * @author mrglass
 *
 */
public class CheckLabelCollisions {
    static class Node {
        String id;
        String[] labels;
        String[] types;
        double[] popularityScores;
        String detail;
    }
    
    static void addDetail(Map<String,StringBuilder> id2detail, String[] trip, int forArg) {
        String u0 = UnaryGroundTruth.toUnaryRelation(trip, forArg);
        StringBuilder buf0 = id2detail.computeIfAbsent(trip[forArg], k -> new StringBuilder());
        if (buf0.length() < 100) {
            buf0.append(u0+"; ");
        } else if (buf0.charAt(buf0.length()-1) != '.') {
            buf0.append("...");
        }
    }
    
    static Map<String,Node> loadNodes(String kbDir) {
        Map<String,Node> id2nodes = new HashMap<>();
        for (String[] lbls : new SimpleTsvIterable(new File(kbDir, KBFiles.labelsTsv))) {
            Node n = new Node();
            n.id = lbls[0];
            n.labels = Arrays.copyOfRange(lbls, 1, lbls.length);
            id2nodes.put(n.id, n);
        }
        if (new File(kbDir, KBFiles.popularityTsv).exists())
            for (String[] pop : new SimpleTsvIterable(new File(kbDir, KBFiles.popularityTsv))) {
                Node n = id2nodes.get(pop[0]);
                if (n != null) {
                    n.popularityScores = new double[pop.length-1];
                    for (int i = 1; i < pop.length; ++i)
                        n.popularityScores[i-1] = Double.parseDouble(pop[i]);
                }
            }
        if (new File(kbDir, KBFiles.typesTsv).exists())
            for (String[] typ : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
                Node n = id2nodes.get(typ[0]);
                if (n != null)
                    n.types = Arrays.copyOfRange(typ, 1, typ.length);
            }
        Map<String,StringBuilder> id2detail = new HashMap<>();
        for (String[] trip : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
            addDetail(id2detail, trip, 0);
            addDetail(id2detail, trip, 2);
        }
        for (Node n : id2nodes.values()) {
            StringBuilder d = id2detail.get(n.id);
            if (d == null)
                n.detail = "NA";
            else
                n.detail = d.toString();
        }
        return id2nodes;
    }
    
    public static void main(String[] args) {
        String kbDir = args[0];
        Map<String,HashSet<Node>> label2node = new HashMap<>();
        for (Node n : loadNodes(kbDir).values()) {
            for (String lbl : n.labels)
                HashMapUtil.addHS(label2node, lbl.toLowerCase(), n);
        }
        HashMapUtil.removeIf(label2node, e -> e.getValue().size() < 2);
        System.out.println(label2node.size()+" label collisions");
        RandomUtil.Sample<String> sampledCollisions = new RandomUtil.Sample<>(40);
        for (Map.Entry<String, HashSet<Node>> e : label2node.entrySet()) {
            //TODO: some uniform sampling, some idCounts.tsv weighted sampling
            if (sampledCollisions.shouldSave()) {
                StringBuilder buf = new StringBuilder();
                buf.append(e.getKey()).append('\n');
                for (Node n : e.getValue()) {
                    buf.append("   "+n.id+": "+Lang.stringList(n.labels, " || ")+"\n");
                    if (n.types != null)
                        buf.append("      "+Lang.stringList(n.types, " && "));
                    if (n.popularityScores != null)
                        buf.append("      "+DenseVectors.toString(n.popularityScores)+"\n");
                    buf.append("      "+n.detail+"\n");
                }
                sampledCollisions.save(buf.toString());
            }
        }
        System.out.println(Lang.stringList(sampledCollisions, "\n\n"));
    }
}
