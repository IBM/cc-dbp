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

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kb.*;
import com.ibm.research.ai.ki.util.*;

public class MergeNodesDBpedia {
    
    static void filterById(File kbDir, String filename, Set<String> idsToDrop) {
        File f = new File(kbDir, filename);
        String movedFilename = FileUtil.removeExtension(filename)+"M"+FileUtil.getExtension(filename);
        File fmoved = new File(kbDir, movedFilename);
        
        if (!f.exists())
            return;
 
        try {
            Files.move(Paths.get(f.getAbsolutePath()), Paths.get(fmoved.getAbsolutePath()), 
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {throw new Error(e);}
        try (PrintStream out = FileUtil.getFilePrintStream(f.getAbsolutePath())) {
            for (String[] parts : new SimpleTsvIterable(fmoved)) {
                if (!idsToDrop.contains(parts[0]))
                    out.println(Lang.stringList(parts, "\t"));
            }
        }
        
        fmoved.delete();
    }
    
    /**
  KB editing: (this class)
  collapse dbr with case difference in labels into one
  link (by label) dbl to dbr; except numbers where we only take the literal, dropping the dbr  
     * 
     */
    public static void merge(File kbDir) {
        NodePopularity.build(kbDir);
        
        //first load id popularity
        Map<String,Double> id2pop = new HashMap<>();
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.popularityTsv))) {
            id2pop.put(parts[0], Double.parseDouble(parts[1]));
        }
        //then gather (case insensitive) label collisions - 
        Map<String,HashSet<String>> label2ids = new HashMap<>();
        for (String[] lbl : new SimpleTsvIterable(new File(kbDir, KBFiles.labelsTsv))) {
            for (int i = 1; i < lbl.length; ++i) {
                HashMapUtil.addHS(label2ids, lbl[i].toLowerCase(), lbl[0]);
            }
        }
        //canonicalize to the more popular id, also dispreferring the dbl:
        Map<String,String> id2repl = new HashMap<>();
        Set<String> idsDrop = new HashSet<>();
        for (Map.Entry<String, HashSet<String>> e : label2ids.entrySet()) {
            if (e.getValue().size() < 2)
                continue;
            //drop numeric labeled resources
            if (Lang.isDouble(e.getKey())) {
                for (String id : e.getValue()) {
                    if (!id.startsWith("dbl:")) {
                        idsDrop.add(id);
                    }
                }
                continue;
            }
            //canonicalize to the more popular id, also dispreferring the dbl:
            double maxPop = Double.NEGATIVE_INFINITY;
            String bestId = null;
            for (String id : e.getValue()) {
                double pop = Lang.NVL(id2pop.get(id), 0.0);
                if (id.startsWith("dbl:"))
                    pop += -10;
                if (pop > maxPop) {
                    bestId = id;
                    maxPop = pop;
                }
            }
            //map other ids to bestId
            for (String id : e.getValue()) {
                if (id.equals(bestId))
                    continue;
                id2repl.put(id, bestId);
            }
        }
        
        //write new triples.tsv with replaced ids
        File f = new File(kbDir, KBFiles.triplesTsv);
        File fmoved = new File(kbDir, "tripleM.tsv");
        try {
        Files.move(Paths.get(f.getAbsolutePath()), Paths.get(fmoved.getAbsolutePath()), 
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {throw new Error(e);}
        try (PrintStream out = FileUtil.getFilePrintStream(f.getAbsolutePath())) {
            for (String[] trip : new SimpleTsvIterable(fmoved)) {
                if (idsDrop.contains(trip[0]) || idsDrop.contains(trip[2]))
                    continue;
                trip[0] = Lang.NVL(id2repl.get(trip[0]), trip[0]);
                trip[2] = Lang.NVL(id2repl.get(trip[2]), trip[2]);
                out.println(Lang.stringList(trip, "\t"));
            }
        }
        fmoved.delete();
        //write labels.tsv, types.tsv and popularity.tsv without the old ids
        idsDrop.addAll(id2repl.keySet());
        filterById(kbDir, KBFiles.labelsTsv, idsDrop);
        filterById(kbDir, KBFiles.typesTsv, idsDrop);
        filterById(kbDir, KBFiles.popularityTsv, idsDrop);
    }
}
