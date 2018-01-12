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
import com.ibm.research.ai.ki.kbp.unary.*;
import com.ibm.research.ai.ki.util.*;

/**
 * finds relation / argument pairs that occur above a threshold, creating unary relations from these.
 * If idCounts.tsv is present (as from DocEntityStats) it will filter entities that do not occur in the corpus
 * @author mrglass
 *
 */
public class FindUnary {
    public static Map<String,MutableDouble> getUnaryRels(File kbDir, int minCount) {
        Map<String,Set<String>> rel2sup = RelationTaxonomy.getTaxonomy(kbDir);
        Map<String,MutableDouble> possibleUnaryRelationCount = new HashMap<>();
        Map<String,MutableDouble> idCounts = null;
        if (new File(kbDir, KBFiles.idCountsTsv).exists())
            idCounts = SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
        int minNodeCount = 1;
        for (String[] trip : RelationTaxonomy.expandTriplesByTaxonomy(
                new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv)), rel2sup)) 
        {
            if (idCounts == null || SparseVectors.getDefaultZero(idCounts, trip[0]) >= minNodeCount)
                SparseVectors.increase(possibleUnaryRelationCount, UnaryGroundTruth.toUnaryRelation(trip, 0), 1.0);
            if (idCounts == null || SparseVectors.getDefaultZero(idCounts, trip[2]) >= minNodeCount)
                SparseVectors.increase(possibleUnaryRelationCount, UnaryGroundTruth.toUnaryRelation(trip, 2), 1.0);
        }
        SparseVectors.trimByThreshold(possibleUnaryRelationCount, minCount);
        return possibleUnaryRelationCount;
    }
    
    public static void main(String[] args) {
        File kbDir = new File(args[0]);
        int minCount = Integer.parseInt(args[1]);
        
        FileUtil.writeFileAsString(new File(kbDir,"unaryRelationCounts.txt"), SparseVectors.toString(getUnaryRels(kbDir, minCount)));
    }
}
