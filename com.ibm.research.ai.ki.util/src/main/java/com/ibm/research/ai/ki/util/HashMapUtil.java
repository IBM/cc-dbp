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

import java.lang.reflect.*;
import java.util.*;
import java.util.function.*;

/**
 * HashMap Utilities
 * 
 * @author partha and mrglass
 * 
 */
public class HashMapUtil {

  /** adds a Key-Value pair to a HashMap of HashSets
   * @param map
   * @param key
   * @param value
   * @return true if a value was added (false if it already existed)
   */
  public static <K, V> boolean addHS(Map<K, HashSet<V>> map, K key, V value) {
    HashSet<V> hs = map.get(key);
    if (hs == null) {
      hs = new HashSet<V>();   
      map.put(key, hs);
    }
    return hs.add(value);
  }

  /** adds a Key-Value pair to a HashMap of HashSets
   * @param map
   * @param key
   * @param value
   * @return
   */
	public static <K, V> boolean addHS(Map<K, HashSet<V>> map, K key, Iterable<V> value) {
		HashSet<V> hs = map.get(key);
		if (hs == null) {
			hs = new HashSet<V>();
			map.put(key, hs);
		}
		if (value instanceof Collection)
			return hs.addAll((Collection<V>) value);

		boolean changed = false;
		for (V v : value)
			changed |= hs.add(v);
		return changed;
	}
  
  /** adds a Key-Value pair to a HashMap of HashSets
   * @param map
   * @param key
   * @param value
   * @return
   */
  public static <K1, K2, V> boolean addHS(Map<K1, HashMap<K2, HashSet<V>>> map, K1 key1, K2 key2,
          V value) {
	HashMap<K2, HashSet<V>> h = map.get(key1);
    if (h == null) {
      h = new HashMap<K2, HashSet<V>>();
      map.put(key1, h);
    }
    return addHS(h, key2, value);
  } 
  
  /**
   * Adds an entry to an ArrayList which is an entry in a HashMap with a key of type K2, which is a value in a Map with a key of type K1
   * @param map
   * @param key1
   * @param key2
   * @param value
   */
  public static <K1, K2, V> void addAL(Map<K1, HashMap<K2, ArrayList<V>>> map, K1 key1, K2 key2,
          V value) {
    HashMap<K2, ArrayList<V>> h = map.get(key1);
    if (h == null) {
      ArrayList<V> al = new ArrayList<V>();
      al.add(value);
      h = new HashMap<K2, ArrayList<V>>();
      h.put(key2, al);
      map.put(key1, h);
    } else {
      ArrayList<V> al = h.get(key2);
      if (al == null) {
        al = new ArrayList<V>();
        al.add(value);
        h.put(key2, al);
      } else {
        al.add(value);
      }
    }
  }
  
  /**
   * Adds an entry to an ArrayList which is an entry in a Map with a key of type K
   * @param map
   * @param key
   * @param value
   */
  public static <K, V> void addAL(Map<K, ArrayList<V>> map, K key, V value) {
    ArrayList<V> al = map.get(key);
    if (al == null) {
      al = new ArrayList<V>();
    }
    al.add(value);
    map.put(key, al);
  }

	public static <K, V> void addAL(Map<K, ArrayList<V>> map, Map<K,V> toAdd) {
		for (Map.Entry<K, V> e : toAdd.entrySet()) {
			addAL(map,e.getKey(),e.getValue());
		}
	}
  public static <K1, K2, V, M extends Map<K2,V>> void addAL2(Map<K1, HashMap<K2, ArrayList<V>>> map, Map<K1, M> toAdd) {
		for (Map.Entry<K1, M> e : toAdd.entrySet()) {
			K1 key1 = e.getKey();
			HashMap<K2, ArrayList<V>> m2 = map.get(key1);
			if (m2 == null) {
				m2 = new HashMap<K2, ArrayList<V>>();
				map.put(key1, m2);
			}
			addAL(m2, e.getValue());
		}
	}
  
  /**
   * Puts a value to a HashMap against an key of type K2, which itself a value of a HashMap with the key of type K1 
   * @param map
   * @param key1
   * @param key2
   * @param value
   */
  public static <K1, K2, V> V put2(Map<K1, HashMap<K2, V>> map, K1 key1, K2 key2, V value){
    HashMap<K2, V> h = map.get(key1);
    if (h == null){
      h = new HashMap<K2, V>();
      map.put(key1, h);
    }
    return h.put(key2, value);
  }

  /**
   * Reverses keys and values of HashMap that has HashSet as values 
   * @param in - a HashMap of HashSet
   * @return out - a HashMap of HashSet with key-values reversed 
   */
  public static <K, V> HashMap<V, HashSet<K>> reverseHS(Map<K, HashSet<V>> in) {
    HashMap<V, HashSet<K>> out = new HashMap<V, HashSet<K>>();
    for (Map.Entry<K, HashSet<V>> e : in.entrySet()) {
      for (V v : e.getValue()) {
        addHS(out, v, e.getKey());
      }
    }
    return out;
  }
  
  
  /**
   * Reverses keys and values of HashMap that has ArrayList as value 
   * @param in - a HashMap of ArrayList
   * @return out - a HashMap of ArrayList with key-values reversed 
   */
  public static <K, V> HashMap<V, ArrayList<K>> reverseAL(Map<K, ArrayList<V>> in) {
    HashMap<V, ArrayList<K>> out = new HashMap<V, ArrayList<K>>();
    for (Map.Entry<K, ArrayList<V>> e : in.entrySet()) {
      ArrayList<V> av = e.getValue();
      for (V v : av) {
        addAL(out, v, e.getKey());
      }
    }
    return out;
  }

  /**
   * Reverses keys and values of a HashMap
   * @param in - a HashMap
   * @return out - a HashMap with key-values reversed
   */
  public static <K, V> HashMap<V, K> reverse(Map<K, V> in) {
    HashMap<V, K> hm = new HashMap<V, K>();
    for (Map.Entry<K, V> e : in.entrySet()) {
      hm.put(e.getValue(), e.getKey());
    }
    return hm;
  }
  
	public static <K, V> HashMap<V, ArrayList<K>> reverseDuplicateValues(Map<K,V> in) {
		HashMap<V, ArrayList<K>> rev = new HashMap<V, ArrayList<K>>();
		for (Map.Entry<K, V> entry : in.entrySet()) {
			addAL(rev, entry.getValue(), entry.getKey());
		}
		return rev;
	}
	public static <K1, K2, V, M extends Map<K2,V>> HashMap<K2, HashMap<K1, V>> reverseDouble(Map<K1, M> dblMap) {
		HashMap<K2, HashMap<K1, V>> revDblMap = new HashMap<K2, HashMap<K1, V>>();
		for (Map.Entry<K1, M> e1 : dblMap.entrySet()) {
			for (Map.Entry<K2, V> e2 : e1.getValue().entrySet()) {
				HashMapUtil.put2(revDblMap, e2.getKey(), e1.getKey(), e2.getValue());
			}
		}
		return revDblMap;
	}
	
	@SafeVarargs
	public static <K, V> HashMap<K,V> fromPairs(Pair<K,V>... pairs) {
		HashMap<K,V> map = new HashMap<K,V>();
		for (Pair<K,V> p : pairs) {
			map.put(p.first, p.second);
		}
		return map;
	}

	public static <K, V> HashMap<K,V> fromPairs(Iterable<Pair<K,V>> pairs) {
		HashMap<K,V> map = new HashMap<K,V>();
		for (Pair<K,V> p : pairs) {
			map.put(p.first, p.second);
		}
		return map;
	}
	
	public static <K,V> HashMap<K,V> fromArrays(K[] keys, V[] values, int length) {
		HashMap<K,V> map = new HashMap<K,V>();
		for (int i = 0; i < length; ++i)
			map.put(keys[i], values[i]);
		return map;
	}
	
	public static <K,V> String toString(Map<K,V> map) {
		if (map == null)
			return "null";
		StringBuilder buf = new StringBuilder();
		int longestKey = 0;
		ArrayList<Pair<String,String>> pairs = new ArrayList<Pair<String,String>>();
		for (Map.Entry<K, V> e : map.entrySet()) {
			String kstr = e.getKey().toString();
			pairs.add(Pair.of(kstr, e.getValue().toString()));
			longestKey = Math.max(longestKey, kstr.length());
		}
		Collections.sort(pairs, new FirstPairComparator());
		for (Pair<String,String> p : pairs) {
			buf.append(Lang.LPAD(p.first, longestKey+1)+" -> "+p.second+"\n");
		}
		return buf.toString();
	}
	
	/**
	 * gets the index (0 based) for key if it is present, otherwise assigns it the index = k2ndx.size()
	 * @param k2ndx
	 * @param key
	 * @return
	 */
	public static <K> int getIndex(Map<K,Integer> k2ndx, K key) {
		Integer ndx = k2ndx.get(key);
		if (ndx == null) {
			ndx = k2ndx.size();
			k2ndx.put(key, ndx);
		}
		return ndx;
	}
	
	/**
	 * Entries in the index must be numbered from 0 to k2ndx.size()-1
	 * @param k2ndx
	 * @param clz
	 * @return
	 */
	public static <K> K[] indexToArray(Map<K,Integer> k2ndx, Class<K> clz) {
		@SuppressWarnings("unchecked")
		K[] ar = (K[])Array.newInstance(clz, k2ndx.size());
		for (Map.Entry<K, Integer> e : k2ndx.entrySet())
			ar[e.getValue()] = e.getKey();
		return ar;
	}
	
	/**
	 * Produces a map from element to index.
	 * @param arr Elements in the array must be unique.
	 * @return
	 */
	public static <K> Map<K,Integer> arrayToIndex(K[] arr) {
		Map<K,Integer> ndx = new HashMap<>();
		for (int i = 0; i < arr.length; ++i)
			if (ndx.put(arr[i], i) != null)
				throw new IllegalArgumentException("Duplicate key: "+arr[i]);
		return ndx;
	}
	
	public static <K> void retainAll(Map<K,?> map, Collection<K> set) {
		removeIf(map, e -> !set.contains(e.getKey()));
	}
	
	public static <K,V> void removeIf(Map<K,V> map, Predicate<Map.Entry<K, V>> removeCondition) {
		for(Iterator<Map.Entry<K, V>> it = map.entrySet().iterator(); it.hasNext(); ) {
		    if (removeCondition.test(it.next()))
		    	it.remove();
		}
	}
	
	public static <K> void removeAll(Map<K,?> map, Collection<K> set) {
		for (K r : set) {
			map.remove(r);
		}
	}
	
	public static <K,V> ArrayList<Pair<K,V>> toPairs(Map<K,V> map) {
		ArrayList<Pair<K,V>> list = new ArrayList<Pair<K,V>>();
		for (Map.Entry<K, V> entry : map.entrySet()) {
			list.add(Pair.of(entry.getKey(), entry.getValue()));
		}
		return list;
	}	
	
	public static <K> K canonical(Map<K,K> map, K item) {
		K canon = map.get(item);
		if (canon != null)
			return canon;
		map.put(item, item);
		return item;
	}
	
	public static <K extends Comparable<K>> Map<K,Integer> toSortedIndex(Iterable<K> items) {		
		Set<K> itemSet = new HashSet<K>();
		for (K item : items) {
			itemSet.add(item);
		}
		
		List<K> itemSorted = new ArrayList<>(itemSet);
		Collections.sort(itemSorted);
		
		Map<K,Integer> index = new HashMap<>();
		for (K item : itemSorted)
			index.put(item, index.size());
		return index;
	}
}


