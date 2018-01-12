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
/*
Copyright (c) 2012 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.ibm.research.ai.ki.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.stat.correlation.*;
import org.apache.commons.math3.stat.ranking.NaturalRanking;


public class DenseVectors {
	public static double klDivergence(double[] v1, double[] v2) {
		return klDivergence(v1,v2,0);
	}
	public static double klDivergence(double[] v1, double[] v2, double smooth) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException();
		double smoothNorm1 = oneNorm(v1) + v1.length * smooth;
		double smoothNorm2 = oneNorm(v2) + v2.length * smooth;
		double kl = 0;
		for (int i = 0; i < v1.length; ++i) {		
			double pi = ((v1[i]+smooth)/smoothNorm1);
			double qi = ((v2[i]+smooth)/smoothNorm2);
			if (pi != 0)
				kl += Math.log(pi/qi) * pi;
		}
		return kl;
	}
	
	public static double[] fromString(String str) {
		if (str.startsWith("[") && str.endsWith("]"))
			str = str.substring(1, str.length()-1).trim();
		return fromString(str, "[\\s,]+");
	}
	
	public static double[] fromString(String str, String sep) {
		if (str.isEmpty())
			return new double[0];
		String[] parts = str.split(sep);
		return fromStrings(parts);
	}
	public static double[] fromStrings(String[] strs) {
		double[] v = new double[strs.length];
		for (int i = 0; i < v.length; ++i)
			v[i] = Double.parseDouble(strs[i]);
		return v;
	}
	
	public static double[] fromSparse(Map<? extends Comparable,Double> sparse) {
		ArrayList<Comparable> keySet = new ArrayList<Comparable>(sparse.keySet());
		Collections.sort(keySet);
		double[] v = new double[keySet.size()];
		for (int i = 0; i < v.length; ++i) {
			v[i] = sparse.get(keySet.get(i));
		}
		return v;
	}
	
	public static double oneNorm(double[] v) {
		double n = 0;
		for (double vi : v) {
			n += Math.abs(vi);
		}
		return n;
	}
	
	public static double twoNorm(double[] v) {
		double n = 0;
		for (double vi : v) {
			n += vi * vi;
		}
		return Math.sqrt(n);
	}
	public static double twoNorm(float[] v) {
		double n = 0;
		for (double vi : v) {
			n += vi * vi;
		}
		return Math.sqrt(n);
	}
	
	public static double twoNorm(double[] v, int offset, int length) {
		double n = 0;
		int end = offset+length;
		for (int i = offset; i < end; ++i)
			n += v[i] * v[i];
		return Math.sqrt(n);
	}
	
	public static boolean hasNaN(double[] v) {
		for (double vi : v)
			if (Double.isNaN(vi) || Double.isInfinite(vi))
				return true;
		return false;
	}
	
	public static double sum(double[] v) {
		double sum = 0;
		for (double vi : v) {
			sum += vi;
		}
		return sum;
	}
	public static float sum(float[] v) {
		float sum = 0;
		for (float vi : v) {
			sum += vi;
		}
		return sum;
	}
	public static int sum(int[] v) {
		int sum = 0;
		for (int vi : v) {
			sum += vi;
		}
		return sum;
	}
	
	public static double mean(double[] v) {
		return sum(v) / v.length;
	}
	
	public static double harmonicMean(double[] v) {
		double invSum = 0;
		for (double vi : v)
			invSum += 1.0/vi;
		invSum /= v.length;
		return 1.0/invSum;
	}
	
	public static int maxIndex(double[] v) {
		int maxNdx = 0;
		double max = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < v.length; ++i) {
			if (v[i] > max) {
				max = v[i];
				maxNdx = i;
			}
		}
		return maxNdx;
	}
	public static int maxIndex(float[] v) {
        int maxNdx = 0;
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < v.length; ++i) {
            if (v[i] > max) {
                max = v[i];
                maxNdx = i;
            }
        }
        return maxNdx;
    }
	public static int minIndex(double[] v) {
		int minNdx = 0;
		double min = Double.POSITIVE_INFINITY;
		for (int i = 0; i < v.length; ++i) {
			if (v[i] < min) {
				min = v[i];
				minNdx = i;
			}
		}
		return minNdx;
	}
	public static int minIndex(float[] v) {
        int minNdx = 0;
        float min = Float.POSITIVE_INFINITY;
        for (int i = 0; i < v.length; ++i) {
            if (v[i] < min) {
                min = v[i];
                minNdx = i;
            }
        }
        return minNdx;
    }
	public static double stdDev(double[] v, double m) {
		double sum = 0;
		for (double vi : v) {
			sum += (vi-m) * (vi-m);
		}
		return Math.sqrt(sum/v.length);
	}
	public static double stdDev(float[] v, float m) {
        double sum = 0;
        for (float vi : v) {
            sum += (vi-m) * (vi-m);
        }
        return Math.sqrt(sum/v.length);
    }
	
	public static HashMap<Integer,MutableDouble> toSparse(double[] v) {
		HashMap<Integer,MutableDouble> s = new HashMap<Integer,MutableDouble>();
		for (int vi = 0; vi < v.length; ++vi) {
			s.put(vi, new MutableDouble(v[vi]));
		}
		return s;
	}
	
	public static double[] extend(double[] v1, double... add) {
		double[] ve = new double[v1.length+add.length];
		System.arraycopy(v1, 0, ve, 0, v1.length);
		System.arraycopy(add, 0, ve, v1.length, add.length);
		return ve;
	}
	
	public static double[] toRanks(double[] x) {
		NaturalRanking ranking = new NaturalRanking();
		return ranking.rank(x);
	}
	
	//Spearman's rank correlation coefficient
	public static double spearmanRho(double[] x, double[] y) {
		SpearmansCorrelation sc = new SpearmansCorrelation();
		return sc.correlation(x, y);
		/*
		if (x.length != y.length || x.length < 2)
			throw new IllegalArgumentException("Invalid lengths "+x.length+" "+y.length);
		int n = x.length;
		double[] xr = toRanksAverageDuplicates(x);
		double[] yr = toRanksAverageDuplicates(y);
		double sumD2 = 0;
		for (int i = 0; i < n; ++i)
			sumD2 += (xr[i] - yr[i]) * (xr[i] - yr[i]);
		return 1.0 - 6.0*sumD2 / (n * (n*n - 1));
		*/
	}
	
	//Kendall's rank correlation
	public static double kendallTau(double[] x, double[] y) {
		KendallsCorrelation kc = new KendallsCorrelation();
		return kc.correlation(x, y);
		/*
		if (x.length != y.length || x.length < 2)
			throw new IllegalArgumentException("Invalid lengths "+x.length+" "+y.length);
		double sum = 0;
		for (int i = 1; i < x.length; ++i) {
			for (int j = 0; j < i; ++j) {
				sum += Math.signum(x[i] - x[j]) * Math.signum(y[i] - y[j]);
			}
		}
		return sum / (0.5 * x.length * (x.length-1));
		*/
	}
	
	public static double pearsonsR(double[] x, double[] y) {
		PearsonsCorrelation pc = new PearsonsCorrelation();
		double corrValue = pc.correlation(x, y);

		if (new Double(corrValue).isNaN())
			corrValue = 0.0;
		
		return corrValue;
	}
	/*
	public static double pearsonsR(double[] v1, double[] v2) {
		double m1 = mean(v1);
		double m2 = mean(v2);
		double s1 = stdDev(v1,m1)+0.0000001;
		double s2 = stdDev(v2,m2)+0.0000001;
		double sum = 0;
		for (int i = 0; i < v1.length; ++i) {
			sum += ((v1[i]-m1)/s1) * ((v2[i]-m2)/s2);
		}
		return (1.0/(v1.length - 1)) * sum;
	}
	*/
	public static void addTo(double[] v, double scalar) {
		for (int i = 0; i < v.length; ++i)
			v[i] += scalar;
	}
	
	//CONSIDER: gratuitously different from BLAS's daxpy
	//actually a number of these would be better done through sai.sgd.dl's BlasImpl
	
	/**
	 * adds v2 to v1, modifying v1
	 * @param v1
	 * @param v2
	 */
	public static void addTo(double[] v1, double[] v2) {
		addTo(v1,v2,1.0);
	}
	/**
	 * adds scaleV2*v2 to v1, modifying v1
	 * @param v1
	 * @param v2
	 * @param scaleV2
	 */
	public static void addTo(double[] v1, double[] v2, double scaleV2) {
		for (int i = 0; i < v1.length; ++i) {
			v1[i] += scaleV2 * v2[i];
		}
	}
	/**
	 * adds scaleV2*v2 to v1, modifying v1
	 * @param v1
	 * @param v2
	 * @param scaleV2
	 */
	public static void addTo(float[] v1, float[] v2, float scaleV2) {
		for (int i = 0; i < v1.length; ++i) {
			v1[i] += scaleV2 * v2[i];
		}
	}
    /**
     * adds scaleV2*v2 to v1, modifying v1
     * @param v1
     * @param v2
     * @param scaleV2
     */
    public static void addTo(int[] v1, int[] v2, int scaleV2) {
        for (int i = 0; i < v1.length; ++i) {
            v1[i] += scaleV2 * v2[i];
        }
    }
	
	
	public static void scale(double[] v, double[] s) {
		for (int i = 0; i < v.length; ++i) {
			v[i] *= s[i];
		}
	}
	
	public static void scale(double[] v, double s) {
		for (int i = 0; i < v.length; ++i) {
			v[i] *= s;
		}
	}
	public static void scale(float[] v, double s) {
		for (int i = 0; i < v.length; ++i) {
			v[i] *= s;
		}
	}
	
	public static double euclidean(double[] v1, double[] v2) {
		double distSum = 0;
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		for (int i = 0; i < v1.length; ++i) {
			distSum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		return Math.sqrt(distSum);
	}
	public static double euclidean(float[] v1, float[] v2) {
		double distSum = 0;
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		for (int i = 0; i < v1.length; ++i) {
			distSum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		return Math.sqrt(distSum);
	}
	
	public static double cosine(double[] v1, double[] v2) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		double numer = 0;
		double len1 = 0;
		double len2 = 0;
		for (int i = 0; i < v1.length; ++i) {
			double d1 = v1[i];
			double d2 = v2[i];
			numer += d1 * d2;
			len2 += d2 * d2;
			len1 += d1 * d1;
		}
		double length = Math.sqrt(len1 * len2);
		if (length == 0) {
			return 0;
		}
		
		double sim = numer / length;
		return sim;
	}
	public static double cosine(float[] v1, float[] v2) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		double numer = 0;
		double len1 = 0;
		double len2 = 0;
		for (int i = 0; i < v1.length; ++i) {
			double d1 = v1[i];
			double d2 = v2[i];
			numer += d1 * d2;
			len2 += d2 * d2;
			len1 += d1 * d1;
		}
		double length = Math.sqrt(len1 * len2);
		if (length == 0) {
			return 0;
		}
		
		double sim = numer / length;
		return sim;
	}
	
	public static void softMax(double[] v) {
		double expsum = 0;
		for (double vi : v)
			expsum += Math.exp(vi);
		for (int i = 0; i < v.length; ++i)
			v[i] = Math.exp(v[i]) / expsum;
	}
	
	public static double dotProduct(double[] v1, double[] v2) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		double dp = 0;
		for (int i = 0; i < v1.length; ++i) {
			dp += v1[i] * v2[i];
		}
		return dp;
	}
	public static double dotProduct(float[] v1, float[] v2) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		double dp = 0;
		for (int i = 0; i < v1.length; ++i) {
			dp += v1[i] * v2[i];
		}
		return dp;
	}
	
	public static double[] toPrimativeArray(Collection<Double> l) {
		double[] d = new double[l.size()];
		int ndx = 0;
		for (Double di : l) {
			d[ndx++] = di;
		}
		return d;
	}

	/**
	 * each dimension distributed uniformly in [0,1)
	 * @param len
	 * @param r
	 * @return
	 */
	public static double[] randomVector(int len) {
		double[] v = new double[len];
		for (int i = 0; i < len; ++i)
			v[i] = Math.random();
		return v;
	}
	
	/**
	 * each dimension distributed uniformly in [0,1)
	 * @param len
	 * @param r
	 * @return
	 */
	public static double[] randomVector(int len, Random r) {
		double[] v = new double[len];
		for (int i = 0; i < len; ++i)
			v[i] = r.nextDouble();
		return v;
	}
	
	/**
	 * get a random vector centered at 0.0
	 * @param len
	 * @param scale 1.0/len or  2.0 / Math.sqrt(len) for random vector initialization is good
	 * @param r
	 * @return
	 */
	public static double[] centeredRandomVector(int len, double scale, Random r) {
		double[] v = new double[len];
		for (int i = 0; i < len; ++i)
			v[i] = scale * (r.nextDouble()-0.5);
		return v;
	}
	
	/**
	 * random vector on the unit hypersphere
	 * @param len
	 * @param r
	 * @return
	 */
	public static double[] sphericallyRandomVector(int len, Random r) {
		double[] v = new double[len];
		for (int i = 0; i < len; ++i)
			v[i] = r.nextGaussian();
		scale(v, 1.0/twoNorm(v));
		return v;
	}
	public static float[] sphericallyRandomFloatVector(int len, Random r) {
		float[] v = new float[len];
		double norm = 0;
		for (int i = 0; i < len; ++i) {
			v[i] = (float)r.nextGaussian();
			norm += v[i] * v[i];
		}
		float scale = (float)(1.0/Math.sqrt(norm));
		for (int i = 0; i < len; ++i)
			v[i] *= scale;
		return v;
	}
	
	public static String toString(int[] v) {
		return Arrays.toString(v);
	}
	
	public static Double[] toObjectArray(double[] v) {
		Double[] vo = new Double[v.length];
		for (int i = 0; i < v.length; ++i)
			vo[i] = v[i];
		return vo;
	}
	
	public static float[] toFloatArray(double[] v) {
		float[] fv = new float[v.length];
		for (int i = 0; i < v.length; ++i)
			fv[i] = (float)v[i];
		return fv;
	}
	
	public static double[] fromFloatArray(float[] fv) {
		double[] v = new double[fv.length];
		for (int i = 0; i < fv.length; ++i)
			v[i] = fv[i];
		return v;
	}
	
	public static double[] fromIntArray(int[] fv) {
		double[] v = new double[fv.length];
		for (int i = 0; i < fv.length; ++i)
			v[i] = fv[i];
		return v;
	}
	
	public static int[] toIntArray(double[] v) {
		int[] iv = new int[v.length];
		for (int i = 0; i < v.length; ++i)
			iv[i] = (int)v[i];
		return iv;
	}
	
	public static double[] fromCollection(Collection<Double> d) {
		double[] v = new double[d.size()];
		int ndx = 0;
		for (Double di : d)
			v[ndx++] = di;
		return v;
	}
	
	/**
	 * same as Arrays.toString, except that 'separator' is used rather than ", "
	 * @param v
	 * @param separator
	 * @return
	 */
	public static String toString(double[] v, String separator) {
		StringBuffer buf = new StringBuffer();
		buf.append("[");
		for (int i = 0; i < v.length; ++i) {
			if (i != 0) {
				buf.append(separator);
			}
			buf.append(v[i]);
		}
		buf.append("]");
		return buf.toString();
	}
	
	/**
	 * same as Arrays.toString
	 * @param v
	 * @return
	 */
	public static String toString(double[] v) {
		return toString(v, ", ");
	}
	
	/**
	 * return an ndx drawn from the cdf 
	 * O(log(cdf.length))
	 * you may also consider using commons.math EnumeratedDistribution
	 * @param cdf a cumulative distribution function, each ndx has probability = cdf[ndx] - cdf[ndx-1]
	 * @param rand
	 * @return
	 */
	public static int randomFromCdf(double[] cdf, Random rand) {
		double cdfProbe = rand.nextDouble();
		int ndx = Arrays.binarySearch(cdf, cdfProbe);
		if (ndx < 0)
			ndx = -(ndx+1);
		return ndx;
	}
	
	/**
	 * return an ndx drawn from the pdf
	 * O(pdf.length)
	 * @param pdf a probability distribution function, each ndx has probability = pdf[ndx]
	 * @param rand
	 * @return
	 */
	public static int randomFromPdf(double[] pdf, Random rand) {
		int ndx = 0;
		double cummulative = 0;
		double val = rand.nextDouble();
		for (ndx = 0; ndx < pdf.length-1; ++ndx) {
			cummulative += pdf[ndx];
			if (val < cummulative)
				break;
		}
		return ndx;
	}
	
	public static double gapFirstAndSecond(double[] v) {
		if (v.length < 2)
			throw new IllegalArgumentException("Cannot compute gap on vector with length "+v.length);
		
		double first = Double.NEGATIVE_INFINITY;
		double second = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < v.length; ++i) {
			if (v[i] > first) {
				second = first;
				first = v[i];
			} else if (v[i] > second) {
				second = v[i];
			}
		}
		return first - second;
	}
}
