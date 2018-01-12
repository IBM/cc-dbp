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

import com.ibm.research.ai.ki.util.*;

/**
 * Shows examples of entities that occur at different frequency ranges, so that a sensible maximum occurrence frequency can be selected, 
 * and possibly a minimum occurrence frequency.
 * @author mrglass
 *
 */
public class ConfigureMinMaxEntityFreq {
    public static void main(String[] args) {
        String kbDir = args[0];
    
        RandomUtil.Sample<String>[] termsByFreq = new RandomUtil.Sample[20];
        for (int i = 0; i < termsByFreq.length; ++i) {
            termsByFreq[i] = new RandomUtil.Sample<>(20);
        }
        Map<String,MutableDouble> idCounts = SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, KBFiles.idCountsTsv)));
        for (Map.Entry<String, MutableDouble> e : idCounts.entrySet()) {
            int bucket = (int)Math.log(e.getValue().value);
            if (bucket < 0) bucket = 0;
            if (bucket >= termsByFreq.length) bucket = termsByFreq.length-1;
            termsByFreq[bucket].maybeSave(Lang.LPAD(""+((int)e.getValue().value), 10)+" "+e.getKey());
        }
        for (int i = 0; i < termsByFreq.length; ++i) {
            System.out.println("=======================================");
            System.out.println(Lang.stringList(termsByFreq[i], "\n"));
        }
    }
}
