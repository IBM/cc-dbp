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
package com.ibm.research.ai.ki.kbp.unary;


import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.formats.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

public class DownsampleEntityFilter implements IEntityFilter {
    private static final long serialVersionUID = 1L;

    protected Map<String,Double> downsampleProb = new HashMap<>();
    protected Set<String> types = null;
    
    @Override
    public void initialize(UnaryGroundTruth gt, RelexConfig config) {
        if (new File(config.convertDir, RelexDatasetFiles.idCountsFile).exists()) {
            double maxCount = config.maxMentionSet * config.maxMentionGroups;
            int idsToDownsample = 0;
            for (String[] parts : new SimpleTsvIterable(new File(config.convertDir, RelexDatasetFiles.idCountsFile))) {
                String id = parts[0];
                double count = Double.parseDouble(parts[1]);
                if (count > 2 * maxCount) {
                    downsampleProb.put(id, (1.5*maxCount)/count);
                    ++idsToDownsample;
                }
            }
            System.out.println("DownsampleEntityFilter: Will downsample "+idsToDownsample+" entities");
        }
        //also support filtering by type
        // check for typeUnary.tsv
        if (new File(config.convertDir, RelexDatasetFiles.typeFilterFile).exists()) {
            types = new HashSet<>();
            for (String t : FileUtil.getLines(new File(config.convertDir, RelexDatasetFiles.typeFilterFile).getAbsolutePath()))
                types.add(t);
            System.out.println("DownsampleEntityFilter: Only considering types: "+Lang.stringList(types, ", "));
        }
    }

    @Override
    public boolean test(String docId, String id, String type) {
        if (types != null && !type.contains(Lang.NVL(type, GroundTruth.unknownType)))
            return false;
        Double dp = downsampleProb.get(id);
        if (dp == null)
            return true;
        //CONSIDER: could base this on docId - but it might not be very robust
        return Math.random() < dp;
    }

}
