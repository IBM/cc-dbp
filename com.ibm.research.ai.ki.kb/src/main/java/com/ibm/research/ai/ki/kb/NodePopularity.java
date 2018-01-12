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
import com.ibm.research.ai.ki.util.*;

/**
 * Builds popularity.tsv: id \t popularity_score (\t alt_popularity_score)*.
 * When dictionary matching, the id with the same label but higher popularity will be preferred.
 * @author mrglass
 *
 */
public class NodePopularity {
    public static void build(File kbDir) {
        Map<String,MutableDouble> id2tripleCount = new HashMap<>();
        Map<String,MutableDouble> relFreqs = new HashMap<>();
        for (String[] trip : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
            SparseVectors.increase(id2tripleCount, trip[0], 1.0);
            SparseVectors.increase(id2tripleCount, trip[2], 1.0);
            SparseVectors.increase(relFreqs, trip[1], 1.0);
        }
        SparseVectors.linearScaleUnitVariance(relFreqs);
        Map<String,MutableDouble> id2tripleCount2 = new HashMap<>();
        for (String[] trip : new SimpleTsvIterable(new File(kbDir, KBFiles.triplesTsv))) {
            double id1Score = 2.0 - 1.0/SparseVectors.getDefaultZero(id2tripleCount, trip[0]);
            double id2Score = 2.0 - 1.0/SparseVectors.getDefaultZero(id2tripleCount, trip[2]);
            double relFreq = SparseVectors.getDefaultZero(relFreqs, trip[1]);
            if (relFreq > 4) relFreq = 4;
            if (relFreq < -4) relFreq = -4;
            double relScore = 1.0 / (1.0 + Math.exp(relFreq));
            SparseVectors.increase(id2tripleCount2, trip[0], relScore*id2Score);
            SparseVectors.increase(id2tripleCount2, trip[2], relScore*id1Score);
        }
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDir, KBFiles.popularityTsv).getAbsolutePath())) {
            for (Map.Entry<String, MutableDouble> e : id2tripleCount2.entrySet()) {
                out.println(e.getKey()+"\t"+e.getValue().value+"\t"+id2tripleCount.get(e.getKey()).value);
            }
        }
    }
}
