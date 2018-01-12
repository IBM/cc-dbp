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
import java.util.Map.Entry;
import java.util.Set;

/**
 * contains methods for operating on Map<?, MutableDouble> which are viewed as sparse vectors, or
 * sparse matrices an entry that is not present is treated as zero
 * 
 * @author partha
 * 
 */
public class SparseVectors {

  /**
   * "It is just the mathematical notion of dot product, sum of the products of each dimension
   * 
   * @param x
   * @param y
   * @return
   */
  public static <K> double dotProduct(Map<K, MutableDouble> x, Map<K, MutableDouble> y) {
    double dotProduct = 0;
    for (Map.Entry<K, MutableDouble> dm1 : x.entrySet()) {
      MutableDouble md = y.get(dm1.getKey());
      if (md != null) {
        dotProduct += dm1.getValue().value * md.value;
      }
    }
    return dotProduct;
  }

  /**
   * scale the vector such that its twoNorm is now 1.0 return the twoNorm before it was scaled
   * 
   * @param m
   * @return
   */
  public static double normalize(Map<?, MutableDouble> m) {
    double twoNorm = twoNorm(m);
    scale(m, 1 / twoNorm);
    return twoNorm;
  }

  /**
   * scale the vector such that its oneNorm is now 1.0 return the oneNorm before it was scaled
   * 
   * @param m
   * @return
   */
  public static double normalizeOne(Map<?, MutableDouble> m) {
    double oneNorm = oneNorm(m);
    scale(m, 1 / oneNorm);
    return oneNorm;
  }

  /**
   * multiply every entry by the scalingFactor
   * 
   * @param m
   * @param scalingFactor
   */
  public static void scale(Map<?, MutableDouble> m, double scalingFactor) {
    for (MutableDouble md : m.values()) {
      md.value *= scalingFactor;
    }
  }

  /**
   * twoNorm = L2 norm = Euclidean norm http://en.wikipedia.org/wiki/Norm_(mathematics)
   * 
   * @param x
   */
  public static double twoNorm(Map<?, MutableDouble> x) {
    double twoNorm = 0;
    for (MutableDouble md : x.values()) {
      twoNorm += md.value * md.value;
    }
    return Math.sqrt(twoNorm);
  }

  /**
   * oneNorm = L1 norm = Manhattan norm http://en.wikipedia.org/wiki/Norm_(mathematics)
   * 
   * @param x
   */
  public static double oneNorm(Map<?, MutableDouble> x) {
    double oneNorm = 0;
    for (MutableDouble md : x.values()) {
      oneNorm += Math.abs(md.value);
    }
    return oneNorm;
  }

  /**
   * return the sum of the entries in the vector
   * 
   * @param c1
   * @return
   */
  public static double sum(Map<?, MutableDouble> c1) {
    double total = 0;
    for (MutableDouble md : c1.values()) {
      total += md.value;
    }
    return total;
  }

  /**
   * return the sum of the entries in the matrix
   * 
   * @param c1
   * @return
   */
  public static double sum2(Map<?, ? extends Map<?, MutableDouble>> c1) {
    double total = 0;
    for (Map<?, MutableDouble> md : c1.values()) {
      total += sum(md);
    }
    return total;
  }

  /**
   * increase the entry corresponding to key by value return true iff the entry was created
   * 
   * @param map
   * @param key
   * @param value
   * @return
   */
  public static <T> boolean increase(Map<T, MutableDouble> map, T key, double value) {
    boolean isCreated = false;
    MutableDouble md = map.get(key);
    if (md == null) {
      md = new MutableDouble();
      map.put(key, md);
      isCreated = true;
    }
    md.value += value;
    return isCreated;
  }

  /**
   * increase the entry corresponding to key1,key2 by value return true iff the entry was created
   * 
   * @param doubleMap
   * @param key1
   * @param key2
   * @param value
   * @return
   */
  public static <S, T> boolean increase(Map<S, HashMap<T, MutableDouble>> doubleMap, S key1,
          T key2, double value) {
    HashMap<T, MutableDouble> md = doubleMap.get(key1);
    if (md == null) {
      md = new HashMap<T, MutableDouble>();
      doubleMap.put(key1, md);
    }
    return increase(md, key2, value);
  }

  /**
   * returns the average value
   * 
   * @param m
   * @return
   */
  public static double getMean(Map<?, MutableDouble> m) {
    double s = 0.0d;
    for (Map.Entry<?, MutableDouble> e : m.entrySet()) {
      s += e.getValue().value;
    }
    return s / m.size();
  }

  /**
   * returns the average value
   * 
   * @param m
   * @return
   */
  public static double getMean2(Map<?, ? extends Map<?, MutableDouble>> m) {
    double s = 0.0d;
    int count = 0;
    for (Map<?, MutableDouble> e : m.values()) {
      for (MutableDouble d : e.values()) {
    	  s += d.value;
    	  ++count;
      }
    }
    return s / count;
  }  
  
  /**
   * returns the variance of the values in m, assuming that their mean is 'mean'
   * 
   * @param m
   * @param mean
   * @return
   */
  public static double getVariance(Map<?, MutableDouble> m, double mean) {
    double s = 0.0d;
    for (Map.Entry<?, MutableDouble> e : m.entrySet()) {
      s += ((e.getValue().value - mean) * (e.getValue().value - mean));
    }
    return s / m.size();
  }

  /**
   * returns the key for the maximum value, null if empty
   * 
   * @param map
   * @return
   */
  public static <T> T maxKey(Map<T, MutableDouble> map) {
    double mdmax = 0;
    Map.Entry<T, MutableDouble> me;
    T retT = null;
    Iterator<Map.Entry<T, MutableDouble>> it = map.entrySet().iterator();
    if (it.hasNext()) {
      me = it.next();
      retT = me.getKey();
      mdmax = me.getValue().value;
    }
    while (it.hasNext()) {
      me = it.next();
      if (me.getValue().value > mdmax) {
        mdmax = me.getValue().value;
        retT = me.getKey();
      }
    }
    return retT;
  }

  /**
   * returns the maximum value, null if empty
   * 
   * @param map
   * @return
   */
  public static MutableDouble maxValue(Map<?, MutableDouble> map) {
    MutableDouble retMd = null;
    @SuppressWarnings("unchecked")
    Iterator<Map.Entry<?, MutableDouble>> it = (Iterator<Map.Entry<?, MutableDouble>>) ((Set<?>) map
            .entrySet()).iterator();
    if (it.hasNext())
      retMd = it.next().getValue();
    while (it.hasNext()) {
      MutableDouble dm = it.next().getValue();
      if (dm.value > retMd.value)
        retMd = dm;
    }
    return retMd;
  }

  /**
   * emoves all entries less than 'removeBelow
   * 
   * @param map
   * @param removeBelow
   */
  public static <K> void trimByThreshold(Map<K, MutableDouble> map, double removeBelow) {
    Iterator<Map.Entry<K, MutableDouble>> e = map.entrySet().iterator();
    while (e.hasNext()) {
      if (e.next().getValue().value < removeBelow) {
        e.remove();
      }
    }
  }

  /**
   * removes all entries less than 'removeBelow', returns total number of entries in the sparse
   * matrix
   * 
   * @param omap
   * @param removeBelow
   * @return
   */
  public static <E> int trimDoubleByThreshold(Map<E, ? extends Map<E, MutableDouble>> doubleMap,
          double removeBelow) {
    int count = 0;
    Set<?> set = doubleMap.entrySet();
    @SuppressWarnings("unchecked")
    Iterator<Entry<E, ? extends Map<E, MutableDouble>>> it = (Iterator<Entry<E, ? extends Map<E, MutableDouble>>>) set
            .iterator();
    while (it.hasNext()) {
      Map<E, MutableDouble> item = it.next().getValue();
      Iterator<Map.Entry<E, MutableDouble>> iter = item.entrySet().iterator();
      while (iter.hasNext()) {
        if (iter.next().getValue().value < removeBelow) {
          iter.remove();
        } else {
          count++;
        }
      }
      if (item.isEmpty())
        it.remove();
    }
    return count;
  }

  /**
   * gets the value for the dimension key. zero if key is not present
   * 
   * @param m
   * @param key
   * @return
   */
  public static <K> double getDefaultZero(Map<K, MutableDouble> m, K key) {
    if (m.get(key) == null)
      return 0.0d;
    else
      return m.get(key).value;
  }

  /**
   * gets the value for the dimension key. zero if key is not present
   * 
   * @param m
   * @param key
   * @return
   */
  public static <S, T, M extends Map<T, MutableDouble>> double getDefaultZero(Map<S, M> m, S key1, T key2) {
    Map<T,MutableDouble> inner = m.get(key1);
    if (inner == null)
    	return 0;
    MutableDouble v = inner.get(key2);
    if (v == null)
    	return 0;
    return v.value;
  }
  
  /**
   * adds the second sparse vector to the first, modifying the first
   * 
   * @param addTo
   * @param toAdd
   */
  public static <T> void addTo(Map<T, MutableDouble> addTo, Map<T, MutableDouble> toAdd) {
    for (Map.Entry<T, MutableDouble> e : toAdd.entrySet()) {
    	SparseVectors.increase(addTo, e.getKey(), e.getValue().value);
    }
  }

	public static <S, T> void addTo2(Map<T, HashMap<S, MutableDouble>> addTo, Map<T, HashMap<S, MutableDouble>> toAdd) {
		for (Map.Entry<T, HashMap<S, MutableDouble>> e1 : toAdd.entrySet()) {
			for (Map.Entry<S, MutableDouble> e2 : e1.getValue().entrySet())
				SparseVectors.increase(addTo, e1.getKey(), e2.getKey(), e2.getValue().value);
		}
	}
  
  /**
   * the values in from are reduced by the corresponding amount in x if newEntries is true, a
   * missing (zero) value in 'from' for 'key' will result in a new entry that is negative x.get(key)
   * only the first sparse vector is modified
   * 
   * @param from
   * @param x
   * @param newEntries
   */
  public static <T> void subtract(Map<T, MutableDouble> from, Map<T, MutableDouble> x,
          boolean newEntries) {
    for (Map.Entry<T, MutableDouble> e : x.entrySet()) {
    	MutableDouble v = from.get(e.getKey());
    	if (v != null) {
    		v.value -= e.getValue().value;
    	} else if (newEntries) {
    		from.put(e.getKey(), new MutableDouble(-e.getValue().value));
    	}
    }
  }

  /**
   * 
   * http://en.wikipedia.org/wiki/Cosine_similarity if it would be NaN, it is instead zero
   * 
   * @param c1
   * @param c2
   * @return
   */
  public static <K> double cosineSimilarity(Map<K, MutableDouble> c1, Map<K, MutableDouble> c2) {
    double numerator = dotProduct(c1, c2);
    double denominator = twoNorm(c1) * twoNorm(c2);
    return denominator > 0 ? numerator / denominator : 0.0;
  }

  /**
   * thresholds is assumed sorted least to greatest. The length of the returned value is one larger
   * than thresholds. The returned value in position i is equal to the number of entries in the
   * sparse vector greater than or equal to thresholds[i-1] and less than thresholds[i]. At the first position
   * the value is equal to the number of entries in the sparse vector less than any threshold at the
   * final position the value is equal to the number of entries greater than any threshold.
   * 
   * @param m
   * @param thresholds
   * @return
   */
	public static int[] getHisto(Map<?, MutableDouble> m, double[] thresholds) {
		return Distribution.getHisto(m.values().stream().map(mv -> mv.value).iterator(), thresholds);
	}



  /**
   * for each entry, one per line, it returns the toString of the key, then a space, then the value
   * the entries are sorted by absolute value, greatest to least the toString of the key is padded
   * with spaces on the left so that the keys line up on the right
   * 
   * @param x
   * @return
   */
  public static <K> String toString(Map<K, MutableDouble> x) {
    return toString(x, x.size());
  }

  /**
   * for each entry, one per line, it returns the toString of the key, then a space, then the value
   * the topN entries are sorted by absolute value, greatest to least the toString of the key is
   * padded with spaces on the left so that the keys line up on the right
   * 
   * @param x
   * @param topN
   * @return
   */
  public static <K> String toString(Map<K, MutableDouble> x, int topN) {
	  if (topN == 0)
		  return "";
    StringBuffer sb = new StringBuffer();
    int bigOne = 0;
    for (Pair<K, MutableDouble> pair : getTopNByValue(x, topN)) {
      bigOne = bigOne < pair.first.toString().length() ? pair.first.toString().length() : bigOne; // find
                                                                                                  // the
                                                                                                  // biggest
                                                                                                  // String
    }

    for (Pair<K, MutableDouble> pair : getTopNByValue(x, topN)) {
      sb.append(Lang.LPAD(pair.first.toString(), bigOne));
      sb.append(" ");
      sb.append(pair.second.value);
      sb.append("\n");
    }
    return sb.toString();
  }

  /**
   * Returns the keys associated with the numDims greatest values
   * 
   * @param map
   * @param numDims
   * @return
   */
  public static <T> Collection<T> getKeyDims(Map<T, MutableDouble> map, int numDims) {
    NBest<Pair<T, MutableDouble>> nBest = new NBest<Pair<T, MutableDouble>>(numDims,
            new SecondPairComparator());
    fillNBest(nBest, map);
    Collection<T> kd = new ArrayList<T>();
    for (Pair<T, MutableDouble> pair : nBest.empty()) {
      kd.add(pair.first);
    }
    return kd;
  }

  
  
  /**
   * returns the sparse vector as a list of pairs, sorted by comp
   * 
   * @param m
   * @return
   */
  public static <T> List<Pair<T, MutableDouble>> sorted(Map<T, MutableDouble> m, SecondPairComparator<T, MutableDouble> comp) {
    List<Pair<T, MutableDouble>> pairs = new ArrayList<Pair<T, MutableDouble>>();
    for (Map.Entry<T, MutableDouble> entry : m.entrySet()) {
      pairs.add(new Pair<T, MutableDouble>(entry.getKey(), entry.getValue()));
    }
    Collections.sort(pairs, comp);
    return pairs;
  }

	/**
	 * least to greatest
	 * @param m
	 * @return
	 */
	public static <T> List<Pair<T, MutableDouble>> sorted(Map<T, MutableDouble> m) {
		return sorted(m, new SecondPairComparator());
	}
  
	/**
	 * greatest to least
	 * @param m
	 * @return
	 */
	public static <T> List<Pair<T, MutableDouble>> sortedReverse(Map<T, MutableDouble> m) {
		return sorted(m, new SecondPairComparator(SecondPairComparator.REVERSE));
	}
  
  /**
   * returns a sorted list with a OverlapRecord for every dimension in which both vectors have a
   * value greater than zero
   * 
   * @param m1
   * @param m2
   * @return
   */
  public static <T> List<OverlapRecord<T>> getOverlap(Map<T, MutableDouble> m1,
          Map<T, MutableDouble> m2) {
    List<OverlapRecord<T>> overlaps = new ArrayList<OverlapRecord<T>>();
    for (Map.Entry<T, MutableDouble> m1Entry : m1.entrySet()) {
      MutableDouble md2 = m2.get(m1Entry.getKey());
      if ((md2 != null) && (m1Entry.getValue().value > 0) && (md2.value > 0)) {
        double overlap = Math.min(m1Entry.getValue().value, md2.value);
        overlaps.add(new OverlapRecord<T>(m1Entry.getKey(), m1Entry.getValue(), md2, overlap));
      }
    }
    Collections.sort(overlaps);
    return overlaps;
  }


  private static <S, T, M extends Map<T, MutableDouble>> void conditionPMI(Map<S, M> coOccurrence,
          Map<S, MutableDouble> firstFreq, Map<T, MutableDouble> secondFreq, double firstTotal,
          double secondTotal, double coTotal,boolean LPMI) {
    for (Map.Entry<S, M> outer : coOccurrence.entrySet()) {
      S s = outer.getKey(); // "p"
      MutableDouble freqS = firstFreq.get(s); // frequency of S
      for (Map.Entry<T, MutableDouble> inner : outer.getValue().entrySet()) {
        T t = inner.getKey(); // "q"
        MutableDouble pmiTarget = inner.getValue(); // this is what we need to fill. Currently pmiTarget.value is p(first & second)*coTotal
        MutableDouble freqT = secondFreq.get(t); // frequency of T
        double numarator = pmiTarget.value / coTotal; // p(first & second) = coOccurrence.get(first).get(second)/coTotal
        double denominator = ((freqS.value / firstTotal) * (freqT.value / secondTotal)); // p(first) *p(second)
        if (LPMI)
        	pmiTarget.value *= Math.log((numarator / denominator)) / Math.log(2); 
        else
        	pmiTarget.value = Math.log((numarator / denominator)) / Math.log(2); 
      }
    }
  }
  /**
   * http://en.wikipedia.org/wiki/Pointwise_mutual_information every entry in the co-occurrence
   * matrix is replaced by the pointwise mutual information of the first key and the second key the
   * frequency of the first key is given in firstFreq, the total is in firstTotal so that p(first)
   * is firstFreq.get(first)/firstTotal; similarly for second coTotal is given in the second so that
   * p(first & second) = coOccurrence.get(first).get(second)/coTotal the pmi(x,y) is log(p(x&y) /
   * (p(x) * p(y)) but the matrix gives frequencies
   * 
   * @param coOccurrence
   * @param firstFreq
   * @param secondFreq
   * @param firstTotal
   * @param secondTotal
   * @param coTotal
   */  
  public static <S, T, M extends Map<T, MutableDouble>> void conditionPMI(Map<S, M> coOccurrence,
          Map<S, MutableDouble> firstFreq, Map<T, MutableDouble> secondFreq, double firstTotal,
          double secondTotal, double coTotal) {
	  conditionPMI(coOccurrence, firstFreq, secondFreq, firstTotal, secondTotal, coTotal, false);
  }
  public static <S, T, M extends Map<T, MutableDouble>> void conditionLPMI(Map<S, M> coOccurrence,
          Map<S, MutableDouble> firstFreq, Map<T, MutableDouble> secondFreq, double firstTotal,
          double secondTotal, double coTotal) {
	  conditionPMI(coOccurrence, firstFreq, secondFreq, firstTotal, secondTotal, coTotal, true);
  } 
  /**
   * every entry in the co-occurrence matrix is replaced by the pointwise mutaul information of the
   * first key and the second key the frequency of the first key is given in firstFreq, the total is
   * in firstTotal so that p(first) is firstFreq.get(first)/firstTotal; similarly for second coTotal
   * is given in the second so that p(first & second) = coOccurrence.get(first).get(second)/coTotal
   * in this version, the coOccurrence matrix is assumed to be complete, so that the firstFreq,
   * secondFreq and all totals can be inferred from coOccurrence
   * 
   * @param coOccurrence
   */  
  public static <S, T, M extends Map<T, MutableDouble>> void conditionPMI(Map<S, M> coOccurrence) {
	  conditionPMI(coOccurrence, false);
  }
  public static <S, T, M extends Map<T, MutableDouble>> void conditionLPMI(Map<S, M> coOccurrence) {
	  conditionPMI(coOccurrence, true);
  }

  private static <S, T, M extends Map<T, MutableDouble>> void conditionPMI(Map<S, M> coOccurrence, boolean LPMI) {
    Map<S, MutableDouble> firstFreq = new HashMap<S, MutableDouble>();
    Map<T, MutableDouble> secondFreq = new HashMap<T, MutableDouble>();
    /*
     * The below process gets (for example) 
     *    p(x) p(y) 
     * 0   a    c 
     * 1   b    d
     * 
     * from:
     * 
     * x y p(x, y) 
     * 0 0 a1c1 
     * 0 1 a2c2 
     * 1 0 b1d1 
     * 1 1 b2d2
     */
    double coTotal = 0;
    for (Map.Entry<S, M> outer : coOccurrence.entrySet()) {
      S s = outer.getKey(); // p
      for (Map.Entry<T, MutableDouble> inner : outer.getValue().entrySet()) {
        T t = inner.getKey(); // q
        MutableDouble pmiTarget = inner.getValue();
        SparseVectors.increase(firstFreq, s, pmiTarget.value);
        SparseVectors.increase(secondFreq, t, pmiTarget.value);
        coTotal +=pmiTarget.value;
      }
    }
    conditionPMI(coOccurrence, firstFreq, secondFreq, coTotal, coTotal, coTotal, LPMI); // scaling factors do matter 
  }

  /**
   * Private method for sorting the Map<K,MutableDouble> types using the values.
   * 
   * @param x
   */
  private static <K> List<Pair<K, MutableDouble>> getTopNByValue(Map<K, MutableDouble> sv, int topN) {
    NBest<Pair<K, MutableDouble>> nBest = new NBest<Pair<K, MutableDouble>>(topN,
            new Comparator<Pair<K, MutableDouble>>() {
              @Override
              public int compare(Pair<K, MutableDouble> o1, Pair<K, MutableDouble> o2) {
                if (Math.abs(o1.second.value) == Math.abs(o2.second.value)) {
                  return 0;
                } else {
                  return Math.abs(o1.second.value) > Math.abs(o2.second.value) ? 1 : -1;
                }
              }
            });
    fillNBest(nBest, sv);
    return nBest.empty();
  }

  private static <K> void fillNBest(NBest<Pair<K, MutableDouble>> nBest, Map<K, MutableDouble> sv) {
    for (Map.Entry<K, MutableDouble> entry : sv.entrySet()) {
      nBest.add(new Pair<K, MutableDouble>(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * for a key, stores the two values from two sparse vectors. the minimum is the overlap sorts
   * according to decreasing amount of overlap
   * 
   * @param <T>
   */
  public static class OverlapRecord<T> implements Comparable<OverlapRecord<T>> {
    private T key;

    private MutableDouble first;

    private MutableDouble second;

    private double overlap;

    public OverlapRecord(T key, MutableDouble first, MutableDouble second, double overlap) {
      this.key = key;
      this.first = first;
      this.second = second;
      this.overlap = overlap;
    }

    public T getKey() {
      return key;
    }

    public MutableDouble getFirst() {
      return first;
    }

    public MutableDouble getSecond() {
      return second;
    }

    public double getOverlap() {
      return overlap;
    }

    @Override
    public int compareTo(OverlapRecord<T> that) {
      if (this.overlap == that.overlap) {
        return 0;
      }
      return this.overlap < that.getOverlap() ? 1 : -1;
    }

    @Override
    public String toString() {
      return key + "\t" + Lang.LPAD(first.toString(), 10) + "\t" + Lang.LPAD(second.toString(), 10)
              + "\t" + Lang.LPAD(String.valueOf(overlap), 10);
    }
	public String toString(int keyPad) {
		return Lang.LPAD(key.toString(), keyPad) + "\t" + Lang.LPAD(first.toString(), 10) + "\t" + Lang.LPAD(second.toString(), 10)
	              + "\t" + Lang.LPAD(String.valueOf(overlap), 10);
	}
  }
  
	public static <T> void toTSVString(Map<T,MutableDouble> m, String filename) {
		PrintStream out = null;
		FileUtil.ensureWriteable(new File(filename));
		try {
			out = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			out.println(e.getKey().toString()+'\t'+e.getValue());
		}
		out.close();
	}
	public static <T> String toTSVString(Map<T,MutableDouble> m) {
		StringBuffer buf = new StringBuffer();
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			buf.append(e.getKey().toString()+'\t'+e.getValue()+'\n');
		}
		return buf.toString();
	}
	public static HashMap<String,MutableDouble> fromTSVString(String tsv, double min) {
		String[] lines = tsv.split("\n");
		return fromTSVString(Arrays.asList(lines), min);
	}
	public static HashMap<String,MutableDouble> fromTSVString(Iterable<String> lines, double min) {
		HashMap<String,MutableDouble> m = new HashMap<String,MutableDouble>();
		for (String line : lines) {
			int tbNdx = line.lastIndexOf('\t');
			double v = Double.parseDouble(line.substring(tbNdx+1));
			if (v >= min)
				m.put(line.substring(0,tbNdx), new MutableDouble(v));
		}
		return m;
	}
	
	public static <T> boolean equals(Map<T,MutableDouble> m1, Map<T,MutableDouble> m2) {
		if (m1.size() != m2.size())
			return false;
		for (Map.Entry<T, MutableDouble> e : m1.entrySet()) {
			MutableDouble d2 = m2.get(e.getKey());
			if (d2 == null)
				return false;
			if (e.getValue().value != d2.value)
				return false;
		}
		return true;
	}  
	public static <T> HashMap<T,MutableDouble> copyValues(Map<T,MutableDouble> map) {
		HashMap<T,MutableDouble> cmap = new HashMap<T,MutableDouble>();
		for (Map.Entry<T, MutableDouble> e : map.entrySet()) {
			cmap.put(e.getKey(), new MutableDouble(e.getValue().value));
		}
		return cmap;
	}
	
	public static <S,T,M extends Map<T,MutableDouble>> void trimDimensions2(Map<S,M> coOccurrence, double retainLength) {
		for (Map.Entry<S, M> e : coOccurrence.entrySet()) {
			Map<T,MutableDouble> map = e.getValue();
			double norm = twoNorm(map);
			if (norm > 0){
				double dropLength = norm * (1 - retainLength);
				List<Pair<T,MutableDouble>> sortedDim = sorted(map);
				double lengthSeen = 0.0;
				for(Pair<T,MutableDouble> dim : sortedDim) {
					lengthSeen += Math.pow(dim.second.value, 2);
					if (lengthSeen > dropLength) break;
					else map.remove(dim.first);
				}
			}
		}
	}
	
	public static <T> void trimDimensions(Map<T,MutableDouble> map, double retainLength) {
		double norm = twoNorm(map);
		if (norm > 0){
			double dropLength = norm * (1 - retainLength);
			List<Pair<T,MutableDouble>> sortedDim = sorted(map);
			double lengthSeen = 0.0;
			for(Pair<T,MutableDouble> dim : sortedDim) {
				lengthSeen += Math.pow(dim.second.value, 2);
				if (lengthSeen > dropLength) break;
				else map.remove(dim.first);
			}
		}	
	}	
	public static <T> void trimToTopN(Map<T,MutableDouble> map, int maxSize) {
		if (maxSize >= map.size())
			return;
		List<Pair<T,MutableDouble>> sortedDim = sorted(map);
		int numDimsToTrim = map.size() - maxSize;
		for(Pair<T,MutableDouble> dim : sortedDim) {
			map.remove(dim.first);
			if (--numDimsToTrim <= 0)
				break;
		}
	}
	
	public static <T> HashMap<T,MutableDouble> fromImmutable(Map<T,Double> im) {
		HashMap<T,MutableDouble> m = new HashMap<T,MutableDouble>();
		for (Map.Entry<T, Double> e : im.entrySet()) {
			m.put(e.getKey(), new MutableDouble(e.getValue()));
		}
		return m;
	}
	
	public static <T> HashMap<T,Double> toImmutable(Map<T,MutableDouble> m) {
		HashMap<T,Double> im = new HashMap<T,Double>();
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			im.put(e.getKey(), e.getValue().value);
		}
		return im;
	}
	public static <T> double[] toDense(Map<T, MutableDouble> m, Map<T, Integer> ndx) {
		int maxKey = 0;
		for (Integer k : ndx.values()) {
			if (k > maxKey) maxKey = k;
			if (k < 0) throw new IllegalArgumentException("All keys must be non-negative");
		}
		double[] dense = new double[maxKey+1];
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			dense[ndx.get(e.getKey())] = e.getValue().value;
		}
		return dense;
	}
	public static double[] toDense(Map<Integer, MutableDouble> m) {
		int maxKey = 0;
		for (Integer k : m.keySet()) {
			if (k > maxKey) maxKey = k;
			if (k < 0) throw new IllegalArgumentException("All keys must be non-negative");
		}
		double[] dense = new double[maxKey+1];
		for (Map.Entry<Integer, MutableDouble> e : m.entrySet()) {
			dense[e.getKey()] = e.getValue().value;
		}
		return dense;
	}
		
	public static <T> boolean setMax(Map<T,MutableDouble> map, T key, double maybeMax) {
		MutableDouble old = map.get(key);
		if (old == null) {
			map.put(key, new MutableDouble(maybeMax));
			return true;
		}
		old.value = Math.max(old.value, maybeMax);
		return false;
	}
	
	public static <K> MutableDouble get(Map<K,MutableDouble> map, K key, double deflt) {
		MutableDouble value = map.get(key);
		if (value == null) {
			value = new MutableDouble(deflt);
			map.put(key, value);
		}
		return value;
	}
	
	public static <K1,K2> MutableDouble get(Map<K1,HashMap<K2,MutableDouble>> map, K1 key1, K2 key2, double deflt) {
		HashMap<K2,MutableDouble> inner = map.get(key1);
		if (inner == null) {
			inner = new HashMap<K2,MutableDouble>();
			map.put(key1, inner);
		}
		return get(inner, key2, deflt);
	}
	public static <T> void set(Map<T,MutableDouble> toSet, Map<T,MutableDouble> values) {
		toSet.clear();
		for (Map.Entry<T, MutableDouble> e : values.entrySet()) {
			toSet.put(e.getKey(), new MutableDouble(e.getValue().value));
		}
	}
	public static <T> boolean set(Map<T,MutableDouble> map, T key, double setTo) {
		MutableDouble old = map.get(key);
		if (old == null) {
			map.put(key, new MutableDouble(setTo));
			return true;
		}
		old.value = setTo;
		return false;
	}
	public static <S,T> boolean set(Map<S,HashMap<T,MutableDouble>> map, S key1, T key2, double val) {
		HashMap<T,MutableDouble> v = map.get(key1);
		if (v == null) {
			v = new HashMap<T,MutableDouble>();
			map.put(key1, v);
		}
		return set(v, key2, val);
	}
	public static MutableDouble minValue(Map<?, MutableDouble> map) {
		MutableDouble min = null;
		for (MutableDouble d : map.values()) {
			if (min == null || (d != null && min.value > d.value)) {
				min = d;
			}
		}
		return min;
	}	
	public static <K> void trimByThresholdAbs(Map<K, MutableDouble> hashMap, double removeBelow) {	
		Iterator<Map.Entry<K, MutableDouble>> it = (Iterator<Map.Entry<K, MutableDouble>>)hashMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<K, MutableDouble> entry = it.next();
			if (Math.abs(entry.getValue().value) < removeBelow) {
				it.remove();
			}
		}
	}
	
	public static HashMap<String,MutableDouble> fromString(String map) {
		HashMap<String,MutableDouble> m = new HashMap<String,MutableDouble>();
		for (String line : map.split("\n")) {
			line = line.trim();
			int space = Math.max(line.lastIndexOf(' '), line.lastIndexOf('\t'));
			m.put(line.substring(0,space).trim(), new MutableDouble(Double.parseDouble(line.substring(space+1).trim())));
		}
		return m;
	}	
	public static <T> Pair<T,MutableDouble> maxEntry(Map<T, MutableDouble> map) {
		T maxKey = null;
		MutableDouble max = null;
		for (Map.Entry<T, MutableDouble> e : map.entrySet()) {
			if (max == null || e.getValue().value > max.value) {
				maxKey = e.getKey();
				max = e.getValue();
			}
		}
		if (max == null) {
			return null;
		}
		return Pair.of(maxKey, max);
	}
	
	/**
	 * Rank 1 means key is the maxKey, rank 2 means one entry is greater...
	 * Null when key is not present
	 * @param map
	 * @param key
	 * @return
	 */
	public static <T> Double rank(Map<T,MutableDouble> map, T key) {
		MutableDouble d = map.get(key);
		if (d == null) 
			return null;
		double rank = 1;
		for (MutableDouble o : map.values()) {
			if (o.value > d.value) {
				rank += 1;
			}
			if (o.value == d.value) {
				rank += 0.5;
			}
		}
		return rank - 0.5;
	}
	
	/**
	 * Divides each entry in m by the corresponding entry in div with missing div = 0, adds 1 to each div
	 * @param m
	 * @param div
	 */
	public static <T> void divideByAdd1(Map<T,MutableDouble> m, Map<T,MutableDouble> div) {
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			MutableDouble divd = div.get(e.getKey());
			double divBy = 1.0;
			if (divd != null) {
				divBy = divd.value + 1.0;
			}
			e.getValue().value /= divBy;
		}
	}
	
	public static <T> void divideBy(Map<T,MutableDouble> m, Map<T,MutableDouble> div) {
		for (Map.Entry<T, MutableDouble> e : m.entrySet()) {
			MutableDouble divd = div.get(e.getKey());
			double divBy = 0.0;
			if (divd != null) {
				divBy = divd.value;
			}
			e.getValue().value /= divBy;
		}
	}
	
	public static <T> void linearScaleUnitVariance(Map<T,MutableDouble> m) {
		double mean = getMean(m);
		double variance = getVariance(m, mean);
		
			
		for (MutableDouble x : m.values()) {
			if (variance == 0)
				x.value = 0;
			else
				x.value = (x.value - mean)/variance;
		}
	}	
	public static <S> double euclidean(Map<S, MutableDouble> c1, Map<S, MutableDouble> c2) {
		double sumSqr = 0;
		for (Map.Entry<S, MutableDouble> e1 : c1.entrySet()) {
			S element = e1.getKey();
			MutableDouble vm1 = e1.getValue();

			double vm1Val = vm1.value;
			MutableDouble vm2 = c2.get(element);
			double vm2Val = vm2 == null ? 0.0 : vm2.value;
			sumSqr += (vm1Val - vm2Val) * (vm1Val - vm2Val);
		}
		
		for (Map.Entry<S, MutableDouble> e2 : c2.entrySet()) {
			if (c1.containsKey(e2.getKey()))
				continue;
			double vm2Val = e2.getValue().value;
			sumSqr += vm2Val * vm2Val;
		}
		return Math.sqrt(sumSqr);
	}
	
	public static <S,T,M extends Map<T,MutableDouble>> void trimDouble(Map<S,M> coOccurrence, double minFirst, double minSecond) {
		Map<S, MutableDouble> firstFreq = new HashMap<S, MutableDouble>();
		Map<T, MutableDouble> secondFreq = new HashMap<T, MutableDouble>();
		double total = 0;
		for (Map.Entry<S, M> co1 : coOccurrence.entrySet()) {
			S i1 = co1.getKey();
			for (Map.Entry<T, MutableDouble> co2 : co1.getValue().entrySet()) {			
				T i2 = co2.getKey();
				MutableDouble count = co2.getValue();
				
				SparseVectors.increase(firstFreq, i1, count.value);
				SparseVectors.increase(secondFreq, i2, count.value);
				total += count.value;
			}
		}
		if (minFirst > 0) {
			for (Map.Entry<S,MutableDouble> e : firstFreq.entrySet()) {
				if (e.getValue().value < minFirst) {
					coOccurrence.remove(e.getKey());
				}
			}
		}
		if (minSecond > 0) {
			for (Map.Entry<S, M> e1 : coOccurrence.entrySet()) {
				ArrayList<T> toRemove = new ArrayList<T>();
				for (Map.Entry<T, MutableDouble> e : e1.getValue().entrySet()) {
					if (secondFreq.get(e.getKey()).value < minSecond) {
						toRemove.add(e.getKey());
					}
				}
				for (T r : toRemove) {
					e1.getValue().remove(r);
				}
			}
		}		
	}
	public static <S, T, M extends Map<T, MutableDouble>> int thresholds(Map<S, M> doubleMap, double minValue, double maxValue) {
		Iterator<Map.Entry<S, M>> oit = (Iterator<Map.Entry<S, M>>)doubleMap.entrySet().iterator();
		int newSize = 0;
		while (oit.hasNext()) {
			Map.Entry<S, M> entry = oit.next();
			Map<T, MutableDouble> hashMap = entry.getValue();
			Iterator<Map.Entry<T, MutableDouble>> it = (Iterator<Map.Entry<T, MutableDouble>>)hashMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<T, MutableDouble> e = it.next();
				if (e.getValue().value < minValue) {
					it.remove();
				} else {
					if (e.getValue().value > maxValue) {
						e.getValue().value = maxValue;
					}
					++newSize;
				}
			}
			if (hashMap.isEmpty()) {
				oit.remove();
			}
		}
		return newSize;
	}
	
	public static <S, T, M extends Map<T, MutableDouble>> int thresholdsAbs(Map<S, M> doubleMap, double minValue, double maxValue) {
		Iterator<Map.Entry<S, M>> oit = (Iterator<Map.Entry<S, M>>)doubleMap.entrySet().iterator();
		int newSize = 0;
		while (oit.hasNext()) {
			Map.Entry<S, M> entry = oit.next();
			Map<T, MutableDouble> hashMap = entry.getValue();
			Iterator<Map.Entry<T, MutableDouble>> it = (Iterator<Map.Entry<T, MutableDouble>>)hashMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<T, MutableDouble> e = it.next();
				if (Math.abs(e.getValue().value) < minValue) {
					it.remove();
				} else {
					if (Math.abs(e.getValue().value) > maxValue) {
						e.getValue().value = Math.signum(e.getValue().value) * maxValue;
					}
					++newSize;
				}
			}
			if (hashMap.isEmpty()) {
				oit.remove();
			}
		}
		return newSize;
	}
	public static <T> String toString(List<OverlapRecord<T>> overlap, int topN) {
		Collections.sort(overlap);
		Collections.reverse(overlap);
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		int countDown = topN;
		int longestKey = 0;
		for (OverlapRecord<T> or : overlap) {
			longestKey = Math.max(or.key.toString().length(), longestKey);
			if (--countDown == 0)
				break;
		}
		for (OverlapRecord<T> or : overlap) {
			if (!first)
				buf.append('\n');
			first = false;
			buf.append(or.toString(longestKey));
			if (--topN == 0)
				break;
		}
		return buf.toString();
	}	
	
	public static double binaryJaccard(Map<?, MutableDouble> c1, Map<?, MutableDouble> c2) {
		if (c1 == null || c2 == null) {
			throw new IllegalArgumentException("similarity HashMap vector argument was null");
		}

		HashSet<Object> nonzeros = new HashSet<Object>();
		nonzeros.addAll(c1.keySet());
		nonzeros.addAll(c2.keySet());
		
		if (nonzeros.isEmpty()) {
			throw new IllegalArgumentException("similarity HashMap vector argument was empty");
		}
		
		double minSum = 0;
		double maxSum = 0;
		for (Object i : nonzeros) {
			MutableDouble d1 = c1.get(i);
			MutableDouble d2 = c2.get(i);
			if (d1 == null) {
				maxSum += 1;
			} else if (d2 == null) {
				maxSum += 1;
			} else {
				minSum += 1;
				maxSum += 1;
			}
		}
		if (maxSum == 0) {
			return 0;
		}
		
		double sim = minSum/maxSum;
		if (Double.isNaN(sim)) {
			System.err.println("one of the vectors has a NaN or Infinity value");
			return 0;
		}
		
		return sim;		
	}	
	
	
	
	public static <S, T, M extends Map<T, MutableDouble>> String toStringTable(Map<S, M> matrix) {
		StringBuilder buf = new StringBuilder();
		ArrayList<S> rows = new ArrayList<S>(matrix.keySet());
		HashSet<T> columnSet = new HashSet<T>();
		for (M cols : matrix.values())
			columnSet.addAll(cols.keySet());
		ArrayList<T> columns = new ArrayList<T>(columnSet);
		Collections.sort((List<Comparable>)rows);
		Collections.sort((List<Comparable>)columns);
		
		int longest = 0;	
		for (S row : rows) {
			longest = Math.max(longest, row.toString().length());
			for (T col : columns) {
				longest = Math.max(longest, col.toString().length());
				double v = SparseVectors.getDefaultZero(matrix, row, col);
				longest = Math.max(longest, Lang.dblStr(v).length());
			}
		}
		longest += 1;
		
		buf.append(Lang.LPAD("", longest));
		for (T col : columns) {
			buf.append(Lang.LPAD(col.toString(), longest));
		}
		buf.append('\n');
		
		for (S row : rows) {
			buf.append(Lang.LPAD(row.toString(), longest));
			for (T col : columns) {
				double v = SparseVectors.getDefaultZero(matrix, row, col);
				buf.append(Lang.LPAD(Lang.dblStr(v), longest));
			}
			buf.append('\n');
		}
		
		return buf.toString();
	}

	public static <S, T, M extends Map<T, MutableDouble>> String toTsvTable(Map<S, M> matrix) {
		StringBuilder buf = new StringBuilder();
		ArrayList<S> rows = new ArrayList<S>(matrix.keySet());
		HashSet<T> columnSet = new HashSet<T>();
		for (M cols : matrix.values())
			columnSet.addAll(cols.keySet());
		ArrayList<T> columns = new ArrayList<T>(columnSet);
		Collections.sort((List<Comparable>)rows);
		Collections.sort((List<Comparable>)columns);
			
		buf.append('\t');
		for (T col : columns) {
			buf.append(col.toString().replace("\t", "\\t")).append('\t');
		}
		buf.setCharAt(buf.length()-1, '\n');
		
		for (S row : rows) {
			buf.append(row.toString().replace("\t", "\\t")).append('\t');
			for (T col : columns) {
				double v = SparseVectors.getDefaultZero(matrix, row, col);
				buf.append(Lang.dblStr(v)).append('\t');
			}
			buf.setCharAt(buf.length()-1, '\n');
		}
		
		return buf.toString();
	}
	
	public static <RowKey,ColKey,M extends Map<ColKey, MutableDouble>> Pair<List<RowKey>,List<ColKey>> toSVD_F_SB(String filename, Map<RowKey,M> matrix, boolean longVals) {
		try {
			DataOutputStream out = FileUtil.getDataOutput(filename);
			Pair<List<RowKey>,List<ColKey>> rowsAndCols = SparseVectors.toSVD_F_SB(out, matrix, longVals);
			out.close();
			return rowsAndCols;
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	public static <K> Map<K,MutableDouble> gatherCounts(Iterable<K> items, int maxDistinct, int minCount, boolean exact) {
		//first pass to find maxDistinct most frequent and get get approx counts
		Map<K,MutableDouble> counts = new HashMap<>();
		int interMinCount = 2;
		int interMax = 3 * maxDistinct;
		for (K item : items) {
			if (SparseVectors.increase(counts, item, 1.0)) {
				while (counts.size() > interMax) {
					SparseVectors.trimByThreshold(counts, interMinCount);
					if (counts.size() > interMax)
						++interMinCount;
				}
			}
		}
		//second pass to get exact counts if desired
		if (exact) {
			for (MutableDouble v : counts.values())
				v.value = 0;
			for (K item : items) {
				MutableDouble v = counts.get(item);
				if (v != null)
					v.value += 1.0;
			}
		}
		//trim to ensure minCount and maxDistinct
		SparseVectors.trimByThreshold(counts, minCount);
		while (counts.size() > maxDistinct) {
			++minCount;
			SparseVectors.trimByThreshold(counts, minCount);
		}
		return counts;
	}
	
	/**
	 * writes the matrix to out, matrix is assumed to be row->col->value
	 * row and col are returned in lists
	 * could be much more efficient about its allocation of temporary storage
	 * format described: http://tedlab.mit.edu/~dr/SVDLIBC/SVD_F_SB.html
	 * @param out must be open for writing, not closed by this operation
	 * @param matrix
	 */
	public static <RowKey,ColKey,M extends Map<ColKey, MutableDouble>> Pair<List<RowKey>,List<ColKey>> toSVD_F_SB(DataOutput out, Map<RowKey,M> matrix, boolean longVals) {	
		int maxRow = 0;
		int maxCol = 0;
		long nonZero = 0;
		Map<RowKey,Integer> rowIndices = new HashMap<>();
		Map<ColKey,Integer> colIndices = new HashMap<>();
		Map<Integer, ArrayList<Pair<Integer,Float>>> col2RowNdxVal = new HashMap<>();
		for (Map.Entry<RowKey,M> e1 : matrix.entrySet()) {
			int row = HashMapUtil.getIndex(rowIndices, e1.getKey());
			boolean hasNonZero = false;
			for (Map.Entry<ColKey,MutableDouble> e2 : e1.getValue().entrySet()) {
				if (e2.getValue().value == 0)
					continue;
				int col = HashMapUtil.getIndex(colIndices, e2.getKey());
				if (col < 0)
					throw new IllegalArgumentException();
				maxCol = Math.max(maxCol, col);
				++nonZero;
				hasNonZero = true;
				HashMapUtil.addAL(col2RowNdxVal, col, Pair.of(row, (float)e2.getValue().value));
			}
			if (!hasNonZero)
				continue;
			
			if (row < 0)
				throw new IllegalArgumentException();
			maxRow = Math.max(maxRow, row);
			
		}
		try {
			out.writeInt(maxRow+1);
			out.writeInt(maxCol+1);
			if (longVals) {
				out.writeLong(nonZero);
			} else {
				if (nonZero > Integer.MAX_VALUE)
					throw new Error("Too many non-zero: "+nonZero);
				out.writeInt((int)nonZero);
			}
			for (int col = 0; col <= maxCol; ++col) {
				List<Pair<Integer,Float>> rowNdxVals = Lang.NVL(col2RowNdxVal.get(col), Collections.EMPTY_LIST);
				out.writeInt(rowNdxVals.size());
				for (Pair<Integer,Float> rowNdxVal : rowNdxVals) {
					out.writeInt(rowNdxVal.first);
					out.writeFloat(rowNdxVal.second);
				}
			}
		} catch (Exception e) {
			Lang.error(e);
		}
		List<RowKey> rows = new ArrayList<>();
		for (Map.Entry<RowKey, Integer> e : rowIndices.entrySet())
			Lang.setAtFill(rows, e.getValue(), e.getKey());
		List<ColKey> cols = new ArrayList<>();
		for (Map.Entry<ColKey, Integer> e : colIndices.entrySet())
			Lang.setAtFill(cols, e.getValue(), e.getKey());
		return Pair.of(rows, cols);
	}
}
