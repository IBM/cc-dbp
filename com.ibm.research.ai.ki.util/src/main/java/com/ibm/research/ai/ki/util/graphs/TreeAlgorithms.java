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

/**
 * Algorithms to work on tree-like datastructures
 * @author mrglass
 *
 */
public class TreeAlgorithms {
	/**
	 * returns the path from the node to the root, including the node and the root, starting with the node.
	 * @param n
	 * @param getParent
	 * @return
	 */
	public static <Node> List<Node> pathToRoot(Node n, Function<Node,Node> getParent) {
		List<Node> parents = new ArrayList<>();
		do {
			parents.add(n);
			n = getParent.apply(n);
		} while (n != null);
		return parents;
	}
	/**
	 * Returns the path from each node to the lowest common ancestor, each path includes the node itself.
	 * If one is the ancestor of the other, the ancestor will have a list of length one, containing just itself.
	 * @param n1
	 * @param n2
	 * @param getParent
	 * @return
	 */
	public static <Node> Pair<List<Node>,List<Node>> getPathsToLCA(Node n1, Node n2, Function<Node,Node> getParent) {
		List<Node> parents1 = pathToRoot(n1, getParent);
		List<Node> parents2 = pathToRoot(n2, getParent);
		if (parents1.get(parents1.size()-1) != parents2.get(parents2.size()-1)) {
			throw new IllegalArgumentException("No shared root!");
		}
		int trimOff = 0;
		while (parents1.size() >= 2+trimOff && parents2.size() >= 2+trimOff && 
				parents1.get(parents1.size()-2-trimOff).equals(parents2.get(parents2.size()-2-trimOff))) 
		{
			++trimOff;
		}
		parents1 = parents1.subList(0, parents1.size()-trimOff);
		parents2 = parents2.subList(0, parents2.size()-trimOff);
		return Pair.of(parents1, parents2);
	}
	
	/**
	 * Get the lowest common ancestor for a set of nodes.
	 * @param ns
	 * @param getParent
	 * @return
	 */
	public static <Node> Node getLCA(Collection<Node> ns, Function<Node,Node> getParent) {
		List<List<Node>> parents = new ArrayList<>();
		Node root = null;
		for (Node n : ns) {
			List<Node> p2r = pathToRoot(n, getParent);
			parents.add(p2r);
			
			//check that they all end at the same root
			if (root != null && root != p2r.get(p2r.size()-1))
				throw new IllegalArgumentException("Multiple roots. Nodes are not all in the same tree.");
			root = p2r.get(p2r.size()-1);
		}
		int lca = 0;
		List<Node> fp = parents.remove(parents.size()-1);
		boolean found = false;
		while (!found && lca+2 <= fp.size()) {
			Node lcan = fp.get(fp.size()-2-lca);
			for (List<Node> p : parents) {
				if (lca+2 > p.size() || lcan != p.get(p.size()-2-lca)) {
					found = true;
					break;
				}
			}
			if (!found)
				++lca;
		}
		return fp.get(fp.size()-1-lca);
	}
	
	/**
	 * Starts with n1 and ends with n2. Goes from n1 up to the LCA, then from the LCA down to n2.
	 * Path should always be at least length 2.
	 * @param n1
	 * @param n2
	 * @param getParent
	 * @return
	 */
	public static <Node> List<Node> getPath(Node n1, Node n2, Function<Node,Node> getParent) {
		Pair<List<Node>,List<Node>> paths = getPathsToLCA(n1,n2,getParent);
		for (int i = paths.second.size()-2; i >= 0; --i)
			paths.first.add(paths.second.get(i));
		return paths.first;
	}
	
	private static class NodeString {
		NodeString(String s) {
			this.s = s;
		}
		String s;
		Collection<NodeString> children = new ArrayList<>();
		String toString(int depth) {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < depth; ++i) {
				buf.append("  ");
			}
			buf.append(s).append('\n');
			for (NodeString c : children) {
				buf.append(c.toString(depth+1));
			}
			return buf.toString();
		}
	}
	public static <Node> String toString(Collection<Node> ns, Function<Node,Node> getParent, Function<Node,String> nodeString) {
		NodeString root = null;
		Map<Node,NodeString> n2s = new IdentityHashMap<>();
		for (Node n : ns)
			n2s.put(n, new NodeString(nodeString.apply(n)));
		for (Node n : ns) {
			NodeString nstr = n2s.get(n);
			Node p = getParent.apply(n);
			if (p == null) {
				if (root != null)
					throw new IllegalArgumentException("Not a single root!");
				root = nstr;
			} else {
				NodeString pstr = n2s.get(p);
				pstr.children.add(nstr);
			}
		}
		return root.toString(0);
	}

}
