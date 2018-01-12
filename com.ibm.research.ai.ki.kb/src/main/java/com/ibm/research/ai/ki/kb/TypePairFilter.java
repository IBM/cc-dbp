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

import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.kbp.unary.*;
import com.ibm.research.ai.ki.util.*;

//CONSIDER: pull the actual writing of RelexDatasetFiles.typePairFilterFile into BuildGroundTruth
public class TypePairFilter {
    //load types.tsv, iterate through triples.tsv counting valid type-pairs (unordered)
    //  sample the low count ones and display
    //  select a freq. cutoff
    //  create a type filter for relex dataset construction
    
    public static void binary(File kbDir, int minTypePairFreq) {
        String[] unk = new String[] {GroundTruth.unknownType};
        
        Map<String,String[]> id2types = new HashMap<>();
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
            String id = parts[0];
            String[] types = Arrays.copyOfRange(parts, 1, parts.length);
            id2types.put(id, types);
        }
        
        Map<String,MutableDouble> typePairCount = new HashMap<>();
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
            String id1 = parts[0];
            String id2 = parts[2];
            String t1 = Lang.NVL(id2types.get(id1), unk)[0];
            String t2 = Lang.NVL(id2types.get(id2), unk)[0];
            String typePair = null;
            if (t1.compareTo(t2) < 0) {
                typePair = t1+"\t"+t2;
            } else {
                typePair = t2+"\t"+t1;
            }
            SparseVectors.increase(typePairCount, typePair, 1.0);
        }
        RandomUtil.Sample<String> rareTypePairs = new RandomUtil.Sample<>(100);
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
            String id1 = parts[0];
            String id2 = parts[2];
            String t1 = Lang.NVL(id2types.get(id1), unk)[0];
            String t2 = Lang.NVL(id2types.get(id2), unk)[0];
            String typePair = null;
            if (t1.compareTo(t2) < 0) {
                typePair = t1+"\t"+t2;
            } else {
                typePair = t2+"\t"+t1;
            }
            double freq = typePairCount.get(typePair).value;
            if (freq < 40) {
                rareTypePairs.maybeSave(typePair+"  "+freq+"  "+Lang.stringList(parts, "\t"));
            }
        }
        System.out.println("Rare type pairs:");
        System.out.println(Lang.stringList(rareTypePairs, "\n"));
        //for dbpedia seems like there is no need to trim the less frequent
        SparseVectors.trimByThreshold(typePairCount, minTypePairFreq);
        FileUtil.writeFileAsString(new File(kbDir, RelexDatasetFiles.typePairFilterFile), SparseVectors.toTSVString(typePairCount));        
    }
    
    public static void unary(File kbDir, int minTypeFreq) {
        UnaryGroundTruth ugt = FileUtil.loadObjectFromFile(new File(kbDir, "ugt.ser.gz"));
        Set<String> ids = ugt.buildEntitySetId2Relations().keySet();
        Map<String,MutableDouble> typeFreq = new HashMap<>();
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.typesTsv))) {
            String id = parts[0];
            String type = parts.length > 1 ? parts[1] : GroundTruth.unknownType;
            if (ids.contains(id))
                SparseVectors.increase(typeFreq, type, 1.0);
        }
        System.out.println("Rare types for unary:");
        for (Map.Entry<String, MutableDouble> e : typeFreq.entrySet()) {
            if (e.getValue().value < 40) {
                System.out.println(e.getKey()+" "+((int)e.getValue().value));
            }
        }
        SparseVectors.trimByThreshold(typeFreq, minTypeFreq);
        FileUtil.writeFileAsString(new File(kbDir, RelexDatasetFiles.typeFilterFile), Lang.stringList(typeFreq.keySet(), "\n"));
    }
    
    public static void main(String[] args) {
        File kbDir = new File(args[0]);
        int minTypePairFreq = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        //binary(kbDir, minTypePairFreq);
        unary(kbDir, minTypePairFreq);
    }
}
