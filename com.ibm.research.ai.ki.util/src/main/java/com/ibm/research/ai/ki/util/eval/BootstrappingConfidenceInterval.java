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
package com.ibm.research.ai.ki.util.eval;

import java.util.*;
import java.util.function.*;

import com.ibm.research.ai.ki.util.*;

public class BootstrappingConfidenceInterval {
	
	/**
	 * NOTE: there are well known cases where the bootstrap estimate of a confidence interval is wrong for some metric (max and min for example)
	 * It is generally safe to estimate the confidence interval for statistics that are smooth functions 
	 * @param sample i.i.d. instances
	 * @param numSamples sample.size()^2 is sometimes recommended. do less if sample.size() is large though.
	 * @param conf how confident you want to be in the interval (0.95 is typical)
	 * @param rand
	 * @param metrics some fairly smooth functions to produce a number from a set of instances
	 * @return
	 */
	public static <T> Pair<Double,Double>[] getConfidenceInterval(Collection<T> sample, int numSamples, double conf, Random rand, 
			Function<Collection<T>,Double>... metrics) 
	{
		ArrayList<T> sampleList = null;
		if (sample instanceof ArrayList)
			sampleList = (ArrayList<T>)sample;
		else
			sampleList = new ArrayList<T>(sample);
		
		
		double[][] vals = new double[metrics.length][];
		for (int mi = 0; mi < metrics.length; ++mi)
			vals[mi] = new double[numSamples];
		ArrayList<T> resample = new ArrayList<T>();
		for (int si = 0; si < numSamples; ++si) {
			resample.clear();
			while (resample.size() < sampleList.size()) {
				resample.add(sampleList.get(rand.nextInt(sampleList.size())));
			}
			for (int mi = 0; mi < metrics.length; ++mi)
				vals[mi][si] = metrics[mi].apply(resample);
		}
		int offset = (int)Math.floor(numSamples * (1-conf)/2); //number of extreme samples on each end to ignore
		Pair<Double,Double>[] intervals = new Pair[metrics.length];
		for (int mi = 0; mi < metrics.length; ++mi) {
			Arrays.sort(vals[mi]);
			intervals[mi] = Pair.of(vals[mi][offset], vals[mi][numSamples-1-offset]);
		}
		return intervals;		
	}
	
	public static <T> Pair<Double,Double>[] getConfidenceInterval(Collection<T> sample, int numSamples, double conf, Function<Collection<T>,Double>... metrics) {
		return getConfidenceInterval(sample, numSamples, conf, new Random(), metrics);
	}
}
