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
import com.ibm.research.ai.ki.util.*;


public class FilterByCorpusCount {
    
    public static void main(String[] args) {
        String kbDir = args[0];
        String kbDirFiltered = args[1];
        int minCount = 1;
        if (args.length > 2)
            minCount = Integer.parseInt(args[2]);
        Map<String,MutableDouble> idCounts = SparseVectors.fromString(FileUtil.readFileAsString(new File(kbDir, "idCounts.tsv")));
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDirFiltered, "labels.tsv").getAbsolutePath())) {
            for (String[] lbl : new SimpleTsvIterable(new File(kbDir, "labels.tsv"))) {
                if (SparseVectors.getDefaultZero(idCounts, lbl[0]) >= minCount) {
                    out.println(Lang.stringList(lbl, "\t"));
                }
            }
        }
        try (PrintStream out = FileUtil.getFilePrintStream(new File(kbDirFiltered, "triples.tsv").getAbsolutePath())) {
            for (String[] trip : new SimpleTsvIterable(new File(kbDir, "triples.tsv"))) {
                if (SparseVectors.getDefaultZero(idCounts, trip[0]) >= minCount && SparseVectors.getDefaultZero(idCounts, trip[2]) >= minCount) {
                    out.println(Lang.stringList(trip, "\t"));
                }
            }
        }
    }
}
