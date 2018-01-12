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

/**
 * orders Pairs based on their first element
 * @author partha
 *
 */
public class SecondPairComparator<T1, T2> implements Comparator<Pair<T1,T2>>, Serializable {

  private Comparator comparator;
  boolean reverse;

  public static boolean REVERSE = true;
  
	public static <S extends Pair<?,?>, T extends List<S>> void sortR(T list) {
		SecondPairComparator rev = new SecondPairComparator();
		rev.setReverseOrdering();
		Collections.sort(list, rev);
	}
	
	public static <S extends Pair<?,?>, T extends List<S>> void sort(T list) {
		Collections.sort(list, new SecondPairComparator());
	}
  
  /**
   * default constructor that assumes the second element is Comparable
   */
  public SecondPairComparator () {
   
  }
  
  public SecondPairComparator (boolean reverse) {
	   this.reverse = reverse;
  }
  
  /**
   * constructor that takes a Comparator where null means treat the elements as Comparable
   * @param comp
   */
  public SecondPairComparator (Comparator comparator) {
    this.comparator = comparator;
  }
  
  /**
   * reverses the order
   */
  public void setReverseOrdering() {
    this.reverse = true;
  }
  
  @Override
  public int compare(Pair p1, Pair p2) {
    if (comparator != null) {
      return reverse?comparator.compare(p2.second, p1.second):comparator.compare(p1.second, p2.second);
    } else {
      return reverse?((Comparable) p2.second).compareTo(p1.second): ((Comparable) p1.second).compareTo(p2.second);  //null means treat the elements as Comparable
    }

  }
}