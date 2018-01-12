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
 * Uses Snowball Sampling to gather a set of related nodes from a graph.
 * 
 * See https://en.wikipedia.org/wiki/Snowball_sampling
 * 
 * @author mrglass
 *
 */
public class SnowballSampler {
	//CONSIDER: could have getRelated : T -> List<Pair<T,Double>>

	/**
	 * Options to control how the sample is gathered from the graph
	 * @author mrglass
	 *
	 */
	public static class Options {
		/**
		 * This many initial seeds are added to the sample
		 */
		public int initialSeeds = 1;
		/**
		 * At most this many neighbors of the current sample will be added at each iteration
		 */
		public int maxExtend = 100;
		/**
		 * At most this fraction of the neighbors of the current sample will be added at each iteration
		 */
		public double maxExtendFraction = 0.5;
		/**
		 * If more than this many iterations proceed with no new nodes added to the sample, the sampling will terminate
		 */
		public int maxNoAddCycles = 10;
		/**
		 * The Random used for selecting the neighbors
		 */
		public Random rand = new Random();
	}
	
	/**
	 * Gets a sample of targetSize from the graph. A seed is selected at random from getRandom(). 
	 * Then neighbors of the current sample are added.
	 * Neighbors are more likely to be added if they are neighbors of many nodes in the current sample.
	 * @param getRelated this gets nodes related to the given node
	 * @param getRandom this gets a random node in the graph
	 * @param targetSize the size of the returned sample, or less if a sample that large cannot be made
	 * @return
	 */
	public static <T> Set<T> getSample(Function<T,Iterable<T>> getRelated, Supplier<T> getRandom, int targetSize) {
		return getSample(getRelated, getRandom, targetSize, new Options());
	}
	
	public static <T> Set<T> getSample(Function<T,Iterable<T>> getRelated, Supplier<T> getRandom, int targetSize, Options options) {
		Set<T> sample = new HashSet<>();
		int noAddCycles = 0;
		
		//add random seeds
		while (sample.size() < options.initialSeeds) {
			int startSize = sample.size();
			sample.add(getRandom.get());
			//track how many cycles we go through without adding anything new, if it is too many then break out
			if (sample.size() > startSize) {
				noAddCycles = 0;
			} else {
				++noAddCycles;
				if (noAddCycles > options.maxNoAddCycles)
					break;
			}
		}
		
		noAddCycles = 0;
		RandomUtil.Sample<T> neighbors = new RandomUtil.Sample<T>(options.maxExtend, options.rand);
		while (sample.size() < targetSize) {
			int startSize = sample.size();
			
			//sample the neighbors
			neighbors.clear();
			for (T cur : sample) {
				for (T rel : getRelated.apply(cur)) {
					if (!sample.contains(rel)) {
						neighbors.maybeSave(rel);
					}
				}
			}
			
			//add the sampled neighbors or if there are no neighbors, another random seed
			if (neighbors.isEmpty()) {
				sample.add(getRandom.get());
			} else {
				int neighborLimit = (int)Math.max(options.maxExtendFraction*neighbors.getAccumulatedWeight(), 1.0);
				neighbors.truncateToSize(neighborLimit);
				sample.addAll(neighbors);
			}
			
			//track how many cycles we go through without adding anything new, if it is too many then break out
			if (sample.size() > startSize) {
				noAddCycles = 0;
			} else {
				++noAddCycles;
				if (noAddCycles > options.maxNoAddCycles)
					break;
			}
		}
		return sample;
	}
}
