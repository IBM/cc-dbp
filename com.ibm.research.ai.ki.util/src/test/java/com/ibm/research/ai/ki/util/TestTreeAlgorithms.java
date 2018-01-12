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
package com.ibm.research.ai.ki.util;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.graphs.*;

public class TestTreeAlgorithms {
	static class Node {
		int depth;
		Node parent;
		List<Node> children = new ArrayList<>();
		String name;
		/**
		 * First node in the returned array is the root.
		 * @param numNodes
		 * @return
		 */
		public static Node[] randomTree(int numNodes) {
			Random rand = new Random();
			Node[] nodes = new Node[numNodes];
			for (int i = 0; i < numNodes; ++i) {
				nodes[i] = new Node();
				nodes[i].name = "N"+i;
				if (i > 0) {
					int pi = rand.nextInt(i);
					nodes[pi].children.add(nodes[i]);
					nodes[i].parent = nodes[pi];
				}
			}
			nodes[0].setDepth(0);
			return nodes;
		}
		
		public void setDepth(int lvl) {
			depth = lvl;
			for (Node n : children) {
				n.setDepth(lvl+1);
			}
		}
		
		public String toString() {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < depth; ++i)
				buf.append("  ");
			buf.append(name);
			buf.append('\n');
			for (Node c : children)
				buf.append(c.toString());
			return buf.toString();
		}
	}
	
	@Test
	public void testPathToRoot() {
		Random rand = new Random();
		for (int testi = 0; testi < 100; ++testi) {
			Node[] tree = Node.randomTree(10+rand.nextInt(100));
			Node n = tree[rand.nextInt(tree.length)];
			List<Node> p = TreeAlgorithms.pathToRoot(n, x -> x.parent);
			assertEquals(p.get(0), n);
			assertEquals(p.get(p.size()-1), tree[0]);
			assertEquals(p.size(), n.depth+1);
			Node pi = n;
			for (int i = 0; i < p.size(); ++i) {
				assertEquals(pi, p.get(i));
				pi = pi.parent;
			}
		}
	}
	
	@Test
	public void testLCA() {
		Random rand = new Random();
		for (int testi = 0; testi < 1000; ++testi) {
			Node[] tree = Node.randomTree(10+rand.nextInt(100));
			Set<Node> toLCA = new HashSet<>();
			for (int i = 0; i < 1+rand.nextInt(5); ++i) {
				toLCA.add(tree[rand.nextInt(tree.length)]);
			}
			
			Node lca = TreeAlgorithms.getLCA(toLCA, n -> n.parent);
			
			for (Node tl : toLCA) {
				//confirm that the lca has lower or equal depth to all nodes
				assertTrue(lca.depth <= tl.depth);
				//confirm that the lca is an ancestor or equal for each node
				List<Node> p2r = TreeAlgorithms.pathToRoot(tl, n -> n.parent);
				if (!p2r.contains(lca)) {
					tl.name = "*"+tl.name;
					lca.name = "LCA:"+tl.name;
					for (Node o : toLCA)
						o.name = "*"+o.name;
					System.err.println(tree[0].toString());
				}
				assertTrue(p2r.contains(lca));
			}
		}
	}
	
	@Test
	public void testPathsToLCA() {
		Random rand = new Random();
		for (int testi = 0; testi < 1000; ++testi) {
			Node[] tree = Node.randomTree(10+rand.nextInt(100));
			int anchor1 = rand.nextInt(tree.length);
			int anchor2 = rand.nextInt(tree.length);
			if (anchor1 == anchor2) continue;
			Pair<List<Node>,List<Node>> paths = TreeAlgorithms.getPathsToLCA(tree[anchor1], tree[anchor2], x -> x.parent);
			Node lca = TreeAlgorithms.getLCA(Arrays.asList(tree[anchor1], tree[anchor2]), x -> x.parent);
			assertEquals(paths.first.get(0), tree[anchor1]);
			assertEquals(paths.second.get(0), tree[anchor2]);
			assertEquals(paths.first.get(paths.first.size()-1), lca);
			assertEquals(paths.second.get(paths.second.size()-1), lca);
		}
	}
	
	@Test
	public void testPath() {
		Random rand = new Random();
		for (int testi = 0; testi < 1000; ++testi) {
			Node[] tree = Node.randomTree(10+rand.nextInt(10));
			int anchor1 = rand.nextInt(tree.length);
			int anchor2 = rand.nextInt(tree.length);
			if (anchor1 == anchor2) continue;
			List<Node> path = TreeAlgorithms.getPath(tree[anchor1], tree[anchor2], x -> x.parent);
			assertTrue(path.size() >= 2);
			assertEquals(path.get(0), tree[anchor1]);
			assertEquals(path.get(path.size()-1), tree[anchor2]);
			Node lca = TreeAlgorithms.getLCA(Arrays.asList(tree[anchor1], tree[anchor2]), x -> x.parent);
			assertTrue(path.contains(lca));
			boolean up = true;
			for (int i = 0; i < path.size(); ++i) {
				if (path.get(i) == lca) {
					up = false;
				}
				if (up && i > 1) {
					assertEquals(path.get(i-1).parent, path.get(i));
				} else if (!up && i < path.size()-1) {
					assertEquals(path.get(i), path.get(i+1).parent);
				}
			}
		}
	}
}
