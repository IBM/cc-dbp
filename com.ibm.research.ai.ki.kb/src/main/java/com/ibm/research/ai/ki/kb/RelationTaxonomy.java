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
import java.util.stream.*;

import com.ibm.research.ai.ki.formats.*;


public class RelationTaxonomy {
    
    public static Map<String,Set<String>> getTaxonomy(File kbDir) {
        Map<String,Set<String>> rel2sup = new HashMap<>();
        if (!new File(kbDir, KBFiles.relationTaxonomyTsv).exists())
            return rel2sup;
        for (String[] parts : new SimpleTsvIterable(new File(kbDir, KBFiles.relationTaxonomyTsv))) {
            Set<String> sups = new HashSet<>();
            for (int i = 1; i < parts.length; ++i)
                sups.add(parts[i]);
            rel2sup.put(parts[0], sups);
        }
        return rel2sup;
    }
    
    public static Iterable<String[]> expandTriplesByTaxonomy(Iterable<String[]> trips, Map<String,Set<String>> taxonomy) {
        return new Iterable<String[]>() {
            @Override
            public Iterator<String[]> iterator() {
                Stream<String[]> expanded = java.util.stream.StreamSupport.stream(trips.spliterator(), true).flatMap(t -> 
                {
                    List<String[]> ex = new ArrayList<>();
                    ex.add(t);
                    if (taxonomy != null) {
                        Set<String> sups = taxonomy.get(t[1]);
                        if (sups != null) {
                            for (String s : sups) {
                                String[] st = new String[3];
                                st[0] = t[0];
                                st[2] = t[2];
                                st[1] = s;
                                ex.add(st);
                            }
                        }
                    }
                    return ex.stream();
                });
                return expanded.iterator();
            }
        };
    }
    
}
