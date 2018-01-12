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
package com.ibm.research.ai.ki.util.graphs;

import java.util.*;
import java.util.function.*;

import com.ibm.research.ai.ki.util.*;

public class GraphAlgorithms {
    /**
     * Construct the transitive closure of things related to nodes.
     * @param nodes
     * @param getRelated get the nodes related to a given node
     * @return mapping from the members of nodes, to nodes that they are related to, and nodes those are transitively related to
     */
    public static <Node, IN extends Iterable<Node>> Map<Node,Set<Node>> transitiveClosure(Iterable<Node> nodes, Function<Node,IN> getRelated) {
        Map<Node,Set<Node>> tc = new HashMap<>();
        for (Node ni : nodes) {
            Set<Node> rel = tc.computeIfAbsent(ni, k -> new HashSet<>());
            IN rn = getRelated.apply(ni);
            if (rn != null) {
                for (Node ri : rn) {
                    rel.add(ri);
                }
            }
        }
        
        boolean changed;
        Set<Node> toAdd = new HashSet<>();
        do {
            changed = false;
            for (Map.Entry<Node, Set<Node>> e : tc.entrySet()) {
                toAdd.clear();
                for (Node r : e.getValue()) {
                    toAdd.addAll(Lang.NVL(tc.get(r),Collections.EMPTY_SET));
                }
                if (e.getValue().addAll(toAdd))
                    changed = true;
            }
        } while (changed);
        
        return tc;
    }
}
