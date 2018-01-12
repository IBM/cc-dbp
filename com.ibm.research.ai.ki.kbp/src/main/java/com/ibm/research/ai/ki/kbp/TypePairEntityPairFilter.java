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

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.formats.*;

public class TypePairEntityPairFilter implements IEntityPairFilter {
    private static final long serialVersionUID = 1L;

    protected Set<String> typePairs = new HashSet<>();
    
    @Override
    public void initialize(GroundTruth gt, RelexConfig config) {
        if (!new File(config.convertDir, "typePairs.tsv").exists())
            throw new IllegalArgumentException("No typePairs.tsv file in convertDir");
        for (String[] parts : new SimpleTsvIterable(new File(config.convertDir, RelexDatasetFiles.typePairFilterFile))) {
            String t1 = parts[0];
            String t2 = parts[1];
            typePairs.add(t1+'\t'+t2);
        }
    }

    @Override
    public boolean test(String id1, String type1, String id2, String type2) {
        String tp = null;
        if (type1.compareTo(type2) <= 0) {
            tp = type1+'\t'+type2;
        } else {
            tp = type2+'\t'+type1;
        }
        
        return typePairs.contains(tp);
    }

}
