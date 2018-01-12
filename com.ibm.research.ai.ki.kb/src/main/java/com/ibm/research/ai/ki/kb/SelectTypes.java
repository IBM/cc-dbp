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

import java.util.*;

import com.ibm.research.ai.ki.util.*;

//TODO: use domain / range of relations to predict missing types
public class SelectTypes {
    protected Set<String>[] type2insts;
    protected String[] typenames;
    protected int minTypeSize;
    protected int maxNumberOfTypes;
    
    public boolean verbose = false;
    
    public SelectTypes(Map<String,HashSet<String>> type2insts, int minTypeSize, int maxNumberOfTypes) {
        List<Pair<String,Integer>> typeAndCount = new ArrayList<>();
        for (Map.Entry<String, HashSet<String>> e : type2insts.entrySet()) {
            if (e.getValue().size() >= minTypeSize) {
                typeAndCount.add(Pair.of(e.getKey(), e.getValue().size()));
            }
        }
        SecondPairComparator.sort(typeAndCount);
        this.type2insts = new Set[typeAndCount.size()];
        this.typenames = new String[typeAndCount.size()];
        for (int ti = 0; ti < typeAndCount.size(); ++ti) {
            String type = typeAndCount.get(ti).first;
            this.type2insts[ti] = type2insts.get(type);
            this.typenames[ti] = type;
        }

        this.minTypeSize = minTypeSize;
        this.maxNumberOfTypes = maxNumberOfTypes;
    }
    
    public String[] getTypesByFreq() {
        return typenames;
    }
    
    public Collection<String> computeSimple() {
        //add all the types that have at least minCount
        Map<String, PriorityQueue<Integer>> id2typeSupport = new HashMap<>();
        Set<Integer> selectedTypes = new HashSet<>();
        for (int ti = 0; ti < type2insts.length; ++ti) {
            for (String id : type2insts[ti])
                id2typeSupport.computeIfAbsent(id, k -> new PriorityQueue<>()).add(ti);
            selectedTypes.add(ti);
        }
        
        //remove types with the fewest instances until they all have minCount instances
        int[] type2support = new int[type2insts.length];
        while (true) {
            //for each type, find the number of instances where it is the most specific type
            Arrays.fill(type2support, 0);
            for (PriorityQueue<Integer> ts : id2typeSupport.values()) {
                if (ts.isEmpty())
                    continue;
                Integer mostSpecificType = ts.peek();
                type2support[mostSpecificType] += 1;
            }
            
            //find the type with the fewest most specific instances and has below minTypeSize
            int worstType = -1;
            int worstTypeSize = Integer.MAX_VALUE;
            for (int ti = 0; ti < type2support.length; ++ti) {
                if (!selectedTypes.contains(ti))
                    continue;
                if (type2support[ti] < worstTypeSize) {
                    worstTypeSize = type2support[ti];
                    worstType = ti;
                }
            }
            
            //remove the worst type, or break if there is no bad type
            if (worstTypeSize < minTypeSize || selectedTypes.size() > maxNumberOfTypes) {
                selectedTypes.remove(worstType);
                for (PriorityQueue<Integer> t : id2typeSupport.values()) {
                    t.remove(worstType);
                }
            } else {
                if (verbose) {
                    System.out.println("SelectTypes with at least "+minTypeSize);
                    int sumSupport = 0;
                    for (int ti = 0; ti < type2support.length; ++ti) {
                        if (!selectedTypes.contains(ti))
                            continue;
                        System.out.println(typenames[ti]+" support "+type2support[ti]);
                        sumSupport += type2support[ti];
                    }
                    System.out.println("Total support "+sumSupport+" out of "+id2typeSupport.size());
                }
                break;
            }
        }
        Set<String> selectedTypeNames = new HashSet<>();
        for (Integer st : selectedTypes)
            selectedTypeNames.add(typenames[st]);
        return selectedTypeNames;
    }
}
