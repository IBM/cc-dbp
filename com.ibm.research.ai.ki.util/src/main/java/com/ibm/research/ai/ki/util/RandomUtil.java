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

import java.nio.charset.*;
import java.security.*;
import java.util.*;

public class RandomUtil {
	private static Random rand = new Random();
	public static void setDefaultRandom(Random rand) {
		RandomUtil.rand = rand;
	}
	
	/**
	 * Converts a string deterministically to a number between 0 and 1.
	 * Any set of strings should theoretically be uniformly distributed on this interval.
	 * CONSIDER: return the Random instead of the double?
	 * @param str
	 * @return
	 */
	public static double pseudoRandomFromString(String str) {
	    MessageDigest messageDigest = null;
	    try {
            messageDigest = java.security.MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new Error(e); //should never happen
        }
	    byte[] d = messageDigest.digest(str.getBytes(StandardCharsets.UTF_8));
	    long value = java.nio.ByteBuffer.wrap(d).getLong();
	    return new Random(value).nextDouble();
	}
	
	/**
	   * generates a random int in the range [fromInclusive-toExclusive)
	   * 
	   * @param fromInclusive
	   * @param toExclusive
	   * @return
	   */
	  public static int randomInt(int fromInclusive, int toExclusive) {
		  return randomInt(fromInclusive, toExclusive, rand);
	  }
	
  /**
   * generates a random int in the range [fromInclusive-toExclusive)
   * 
   * @param fromInclusive
   * @param toExclusive
   * @return
   */
  public static int randomInt(int fromInclusive, int toExclusive, Random rand) {
    if (fromInclusive >= toExclusive) {
      throw new IllegalArgumentException("fromInclusive can not be >= toExclusive ");
    }
    int num;
    do {
      num = (int) (fromInclusive + Math.floor(rand.nextDouble() * (toExclusive - fromInclusive)));
    } while (num >= toExclusive);
    return num;
  }

  public static <T> T randomMember(Collection<T> col) {
	  return randomMember(col, rand);
  }
  
	public static int sampleFromCDF(double[] cdf, double p) {
		if (p < 0.0 || p > 1.0)
			throw new IllegalArgumentException("out of range: "+p);
		int ndx = Arrays.binarySearch(cdf, p);
		if (ndx < 0)
			ndx = -(ndx + 1);
		else if (ndx < cdf.length-1)
			++ndx; //our ranges go from [0,cdf[0]), [cdf[0],cdf[1]), etc. So if we land on a value exactly, we take the next ndx.
		if (ndx >= cdf.length)
			throw new Error("bad cdf! " + DenseVectors.toString(cdf));
		return ndx;
	}

	public static int sampleFromCDF(double[] cdf, Random rand) {
		return sampleFromCDF(cdf, rand.nextDouble());
	}

	/**
	 * Returns an index [0, weights.length) with each index having probability proportional to weights[index].
	 * Negative weights are illegal. Weights could be a pdf.
	 * @param weights
	 * @param zero2one if sampling, this would be generated as by Math.random()
	 * @return
	 */
	public static int sampleFromWeighted(double[] weights, double zero2one) {
	    if (weights.length < 0)
	        throw new IllegalArgumentException("must supply weights");
	    double total = DenseVectors.sum(weights);
	    double p = zero2one * total;
	    int ndx = 0;
	    double sum = 0;
	    for (; ndx < weights.length; ++ndx) {
	        sum += weights[ndx];
	        if (sum >= p)
	            break;
	    }
	    return ndx;
	        
	}
	
  /**
   * return a random entry in a collection
   * 
   * @param col
   * @return
   */
  public static <T> T randomMember(Collection<T> col, Random rand) {
    if (col == null || col.size() == 0) {
      return null;
    }
    int index = randomInt(0, col.size(), rand);
    if (col instanceof List)
    	return ((List<T>)col).get(index);
    int counter = 0;
    T ret = null;
    for (T entry : col) {
      if (counter == index) {
        ret = entry;
        break;
      }
      counter++;
    }
    return ret;
  }

  /**
   * remove and return a random member of the passed Collection
   * 
   * @param col
   * @return
   */
  public static <T> T removeRandom(Collection<T> col) {
    T entry = randomMember(col);
    col.remove(entry);
    return entry;
  }

  /**
   * return a random entry in a map
   * 
   * @param map
   * @return
   */
  public static <K, V> Map.Entry<K, V> randomEntry(Map<K, V> map) {
    if (map == null || map.size() == 0) {
      return null;
    }
    int index = randomInt(0, map.size());
    int counter = 0;
    Map.Entry<K, V> ret = null;
    for (Map.Entry<K, V> entry : map.entrySet()) {
      if (counter == index) {
        ret = entry;
      }
      counter++;
    }
    return ret;
  }

  /**
   * A sample of n elements. It is a Collection. uses weighted reservoir sampling
   * http://arxiv.org/pdf/1012.0256.pdf (A-Chao)
   * http://blog.cloudera.com/blog/2013/04/hadoop-stratified-randosampling-algorithm/
   * 
   * @author partha
   * 
   * @param <T>
   */
  public static class Sample<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;

    int sampleSize;
    
    public int getSampleSize() {
		return sampleSize;
	}

	double accumulatedWeight;
    Random rand;

    public Sample(int sampleSize) {
      this.sampleSize = sampleSize;
      this.rand = RandomUtil.rand;
    }
    public Sample(int sampleSize, Random rand) {
        this.sampleSize = sampleSize;
        this.rand = rand;
    }

    public void maybeSave(T item) {
    	maybeSave(item, 1.0);
    }
    
    /**
     * Truncate the sample to no more than targetSize
     * @param targetSize
     */
    public void truncateToSize(int targetSize) {
    	if (targetSize < this.size()) {
    		Collections.shuffle(this, rand);
    		while (targetSize < this.size())
    			this.remove(this.size()-1);
    	}
    }
    
    /**
     * If samples are unweighed this is the number of samples seen so far.
     * @return
     */
    public double getAccumulatedWeight() {
    	return accumulatedWeight;
    }
    
    /**
     * maybeSave(T item, double weight) behaves like two functions: shouldSave and save
     * 
     * @param item
     * @param weight
     */
    public void maybeSave(T item, double weight) {
      if (!isFull()) {
        add(item);
      } else if (shouldSave(weight)) {
        save(item);
      }
    }

    public boolean shouldSave() {
    	return shouldSave(1.0);
    }
    
    /**
     * determines if the item should be stored in the reservoir
     * 
     * @param weight
     * @return boolean
     */
    public boolean shouldSave(double weight) {
      accumulatedWeight += weight;
      if (!isFull()) {
        return true;
      } else if (rand.nextDouble() < calculateProb(weight)) {
        return true;
      }
      return false;
    }

    /**
     * stores the item in the reservoir, ejecting another one if needed
     * @param item
     */
    public void save(T item) {
      if (!isFull()) {
        this.add(item);
      } else {
        set(randomInt(0, sampleSize, rand), item); // randomly replace an entry
      }
    }

    private boolean isFull() {
      return this.size() >= sampleSize;
    }

    private double calculateProb(double weight) {
      return (weight * sampleSize) / accumulatedWeight;
    }
    
    @Override
    public void clear() {
    	super.clear();
    	accumulatedWeight = 0;
    }
  }

  public static <T> T randomFromIterator(Iterator<T> iter) {
	  return randomFromIterator(iter, rand);
  }
	public static <T> T randomFromIterator(Iterator<T> iter, Random rand) {
		if (!iter.hasNext()) {
			return null;
		}
		T element = iter.next();
		double seen = 1;
		while(iter.hasNext()) {
			seen += 1;
			T posReplace = iter.next();
			if (rand.nextDouble() < 1.0/seen) {
				element = posReplace;
			}
		}
		return element;
	}  
	public static <T> T removeRandomFast(ArrayList<T> list, Random rand) {
		int ndx = rand.nextInt(list.size());
		if (ndx == list.size()-1)
			return list.remove(ndx);
		T r = list.get(ndx);
		list.set(ndx, list.remove(list.size()-1));
		return r;
	}
	
	public static <T> ArrayList<T> getSample(Iterable<T> from, int size) {
		return getSample(from, size, RandomUtil.rand);
	}
	
	public static String randomAlphaNumericString(int length, Random rand) {
		char[] buf = new char[length];
		int limit = (58-48) + (91-65) + (123-97);
		for (int i = 0; i < buf.length; ++i) {
			//[48-57] [65-90] [97-122] 
			int code = rand.nextInt(limit);
			if (code < 58-48) {
				code += 48; //digit
			} else if (code < (58-48) + (91-65)) {
				code += 65; //uppercase
			} else {
				code += 97; //lowercase
			}
			buf[i] = (char)code;
		}
		return new String(buf);
	}
	
	public static <T> ArrayList<T> getSample(Iterable<T> from, int size, Random rand) {
		//if it is a long random access list we can take time proportional to the sample size rather than the iterable length
		if (from instanceof List && from instanceof RandomAccess && ((List<T>)from).size() > 10*size) {
			List<T> fromL = (List<T>)from;
			Set<Integer> ndxs = new HashSet<Integer>();
			while (ndxs.size() < size)
				ndxs.add(rand.nextInt(fromL.size()));
			ArrayList<T> s = new ArrayList<>();
			for (Integer ndx : ndxs)
				s.add(fromL.get(ndx));
			return s;
		}
		Sample<T> s = new Sample<T>(size, rand);
		for (T i : from) {
			s.maybeSave(i);
		}
		return s;
	}
	
	public static void shuffleIntArray(int[] s, Random rand) {
		for (int i=0; i < s.length; i++) {
		    int ndx = rand.nextInt(s.length);
		    int tmp = s[i];
		    s[i] = s[ndx];
		    s[ndx] = tmp;
		}
	}
	
	public static int[] toShuffledIntArray(Collection<Integer> c, Random rand) {
		int[] s = new int[c.size()];
		int ndx = 0;
		for (Integer ci : c) {
			if (ndx == 0) {
				s[ndx] = ci;
			} else {
				int j = rand.nextInt(ndx+1);
				s[ndx] = s[j];
				s[j] = ci;
			}
			++ndx;
		}
		return s;
	}
	
  public static void main(String[] args) {
    RandomUtil.Sample<Integer> sample = new RandomUtil.Sample<Integer>(10);
    for (int i=0; i < 1000; i++ ){
      sample.maybeSave(new Integer(i),345433+Math.random());
    }
    for (Integer i: sample){
      System.out.println(i);
    }
  }
}
