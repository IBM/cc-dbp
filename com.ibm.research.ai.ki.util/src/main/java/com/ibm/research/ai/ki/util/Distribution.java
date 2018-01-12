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

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Track an approximation of the distribution of a variable, by maintaining counts for ranges of the value.
 * @author mrglass
 *
 */
public class Distribution implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected double[] thresholds;
	/**
	 * count[i] are counts of things less than thresholds[i] (if i < thresholds.length) and geq than thresholds[i-1] (if i-1 >= 0).
	 * count[0] is less than any threshold.
	 * count[thresholds.length] is the count of values geq than any threshold.
	 */
	protected int[] counts;
	
	public Distribution(double min, double max, int numThresholds) {
		if (min >= max || numThresholds < 1)
			throw new IllegalArgumentException();
		thresholds = new double[numThresholds];
		counts = new int[numThresholds+1];
		double inc = (max-min)/numThresholds;
		for (int i = 0; i < numThresholds; ++i) {
			thresholds[i] = min + inc * i;
		}
	}
	
	public Distribution(double[] thresholds) {
		this.thresholds = Arrays.copyOf(thresholds, thresholds.length);
		Arrays.sort(thresholds);
		this.counts = new int[thresholds.length+1];
	}
	
	/**
	 * Add the value to the distribution
	 * @param val
	 */
	public void add(double val) {
		int ndx = Arrays.binarySearch(thresholds, val);
		if (ndx < 0)
			ndx = -(ndx+1);
		else
			++ndx;
		++counts[ndx];
	}
	
	/**
	 * Adds the distribution to this one. Only valid if the distributions are over the same thresholds.
	 * @param d
	 */
	public void merge(Distribution d) {
		if (d.thresholds.length != this.thresholds.length) {
			throw new IllegalArgumentException("thresholds must match");
		}
		/* don't check this all the time
		for (int i = 0; i < thresholds.length; ++i)
			if (Math.abs(thresholds[i] - d.thresholds[i]) > 0.0001)
				throw new IllegalArgumentException("thresholds must match");
		*/
		for (int i = 0; i < counts.length; ++i)
			counts[i] += d.counts[i];
	}
	
	//TODO: test me
	public double fractionBetween(double min, double max) {
	    double total = 0;
	    double between = 0;
	    for (int i = 0; i < thresholds.length; ++i) {
	        total += counts[i];
	        if (thresholds[i] < min)
	            continue;
	        if (thresholds[i] >= max)
	            continue;
	        between += counts[i];
	    }
	    total += counts[thresholds.length];
	    if (thresholds[thresholds.length-1] < max)
	        between += counts[thresholds.length];
	    
	    return between / total;
	}
	
	/**
	 * Finds the lowest threshold s.t. at least topPercent items are geq than it.
	 * @param topPercent
	 * @return
	 */
	public double thresholdForTopPercent(double topPercent) {
		double total = 0;
		for (int c : counts)
			total += c;
		double sum = 0;
		for (int i = counts.length-1; i >= 1; --i) {
			sum += counts[i];
			if (sum / total >= topPercent)
				return thresholds[i-1];
		}
		return Double.NEGATIVE_INFINITY;
	}

	/**
	 * Finds the lowest threshold s.t. at least topN items are geq than it.
	 * @param topN
	 * @return
	 */
	public double thresholdForTopN(int topN) {
		int sum = 0;
		for (int i = counts.length-1; i >= 1; --i) {
			sum += counts[i];
			if (sum >= topN)
				return thresholds[i-1];
		}
		return Double.NEGATIVE_INFINITY;
	}
	
	/**
	 * pretty print the distribution
	 */
	public String toString() {
		return stringHisto(thresholds, counts);
	}
	
	  /**
	   * thresholds is assumed sorted least to greatest. The length of the returned value is one larger
	   * than thresholds. The returned value in position i is equal to the number of entries in the
	   * sparse vector greater than or equal to thresholds[i-1] and less than thresholds[i]. At the first position
	   * the value is equal to the number of entries in the sparse vector less than any threshold at the
	   * final position the value is equal to the number of entries greater than any threshold.
	   * 
	   * @param vals
	   * @param thresholds
	   * @return
	   */	
	public static int[] getHisto(Iterator<Double> vals, double[] thresholds) {
		int[] ret = new int[thresholds.length + 1];		
		while (vals.hasNext()) {
			double val = vals.next();
			if (val < thresholds[0]) {
				ret[0]++;
			} else if (val >= thresholds[thresholds.length - 1]) {
				ret[thresholds.length]++;
			} else {
				for (int i = 1; i < thresholds.length; i++) {
					if (val >= thresholds[i - 1] && val < thresholds[i]) {
						ret[i]++;
						break;
					}
				}
			}
		}
		return ret;
	}
	
	/**
	 * Produces a nicely formated display of the thresholds and counts. The
	 * number of lines is equal to the number of counts which is equal to the
	 * number of thresholds plus one
	 * 
	 * @param thresholds
	 * @param counts
	 * @return
	 */
	public static String stringHisto(double[] thresholds, int[] counts) {
		StringBuffer sb = new StringBuffer();
		pad10Append("-", sb);
		sb.append("\t");
		pad10Append(String.valueOf(thresholds[0]) + ")", sb);
		sb.append("\t");
		pad10Append(String.valueOf(counts[0]), sb);
		sb.append("\n");
		for (int i = 1; i < thresholds.length; i++) {
			pad10Append("[" + String.valueOf(thresholds[i - 1]), sb);
			sb.append("\t");
			pad10Append(String.valueOf(thresholds[i]) + ")", sb);
			sb.append("\t");
			pad10Append(String.valueOf(counts[i]), sb);
			sb.append("\n");
		}
		pad10Append("[" + String.valueOf(thresholds[thresholds.length - 1]), sb);
		sb.append("\t");
		pad10Append("-", sb);
		sb.append("\t");
		pad10Append(String.valueOf(counts[thresholds.length]), sb);
		return sb.toString();
	}

	private static void pad10Append(String str, StringBuffer sb) {
		sb.append(Lang.LPAD(str, 10));
	}

}