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

public class BuildGroundTruth {
    //CONSIDER: create instance filter class
    //  we take multi-token
    //  we take numbers
    //  we take moderate freq
    
    public static void build(File kbDir, int minCorpusCount, int minUnaryCount, boolean useRelationTaxonomy) {
      //create binary and unary ground truth from a kbDir
        GroundTruth gt = new GroundTruth();
        //if idCounts.tsv exists, we filter by that
        Map<String,MutableDouble> idCounts = null;
        if (new File(kbDir, KBFiles.idCountsTsv).exists())
            idCounts = SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
        
        Map<String,Set<String>> rel2sup = useRelationTaxonomy ? RelationTaxonomy.getTaxonomy(kbDir) : new HashMap<>();;
        
        //just loop through triples.tsv and gt.addRelation
        for (String[] trip : RelationTaxonomy.expandTriplesByTaxonomy(
                new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv)),
                rel2sup)) 
        {
            if (idCounts == null || 
                    (SparseVectors.getDefaultZero(idCounts, trip[0]) >= minCorpusCount &&  
                    SparseVectors.getDefaultZero(idCounts, trip[2]) >= minCorpusCount))
            {
                gt.addRelation(trip[0], trip[2], trip[1]);
            }
        }
        FileUtil.saveObjectAsFile(new File(kbDir, "gt.ser.gz"), gt);

        Map<String,MutableDouble> unaryRels = FindUnary.getUnaryRels(kbDir, minUnaryCount);
        //create unary ground truth too
        Map<String, ArrayList<String>> inst2rels = new HashMap<>();
        for (String[] trip : RelationTaxonomy.expandTriplesByTaxonomy(
                new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv)),
                rel2sup)) 
        {
            String u1 = UnaryGroundTruth.toUnaryRelation(trip, 0);
            String u2 = UnaryGroundTruth.toUnaryRelation(trip, 2);
            if (unaryRels.containsKey(u1) && 
                    (idCounts == null || SparseVectors.getDefaultZero(idCounts, trip[0]) >= minCorpusCount))
            {
                HashMapUtil.addAL(inst2rels, trip[0], u1);
            }
            if (unaryRels.containsKey(u2) && 
                    (idCounts == null || SparseVectors.getDefaultZero(idCounts, trip[2]) >= minCorpusCount))
            {
                HashMapUtil.addAL(inst2rels, trip[2], u2);
            }
        }
        UnaryGroundTruth ugt = UnaryGroundTruth.build(inst2rels, null);
        FileUtil.saveObjectAsFile(new File(kbDir, "ugt.ser.gz"), ugt);
    }
    
    //CONSIDER: take properties file as input - giving minInstanceCount (from idCounts.tsv) minUnaryCount, and whether to use relationTaxonomy.tsv
    public static void main(String[] args) {
        File kbDir = new File(args[0]);
        int minCorpusCount = 1;
        int minUnaryCount = 100;
        boolean useRelationTaxonomy = true;
        
        build(kbDir, minCorpusCount, minUnaryCount, useRelationTaxonomy);

    }
}
