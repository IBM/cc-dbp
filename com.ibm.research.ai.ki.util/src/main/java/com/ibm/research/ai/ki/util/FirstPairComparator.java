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
import java.util.Comparator;

/**
 * orders Pairs based on their first element
 * @author partha
 *
 */
public class FirstPairComparator implements Comparator<Pair>, Serializable {

  private Comparator comparator;
  boolean reverse;
  
  /**
   * default constructor that assumes the first element is Comparable
   */
  public FirstPairComparator() {
   
  }
  /**
   * constructor that takes a Comparator where null means treat the elements as Comparable
   * @param comp
   */
  public FirstPairComparator(Comparator comparator) {
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
	  if (p1 == null) throw new IllegalArgumentException();
	  if (p2 == null) throw new IllegalArgumentException();
    if (comparator != null) {
      return reverse?comparator.compare(p2.first, p1.first):comparator.compare(p1.first, p2.first);
    } else {
      return reverse?((Comparable) p2.first).compareTo(p1.first): ((Comparable) p1.first).compareTo(p2.first);  //null means treat the elements as Comparable
    }
  }
}
