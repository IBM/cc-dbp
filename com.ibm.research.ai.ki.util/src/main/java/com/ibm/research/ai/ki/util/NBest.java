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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class NBest<T> extends PriorityQueue<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  protected int limit;

  /**
   * constructor that takes int limit
   * 
   * @param limit
   */
  public NBest(int limit) {
    super(limit);
    this.limit = limit;
  }

  /**
   * constructor that takes limit and Comparator
   * 
   * @param limit
   * @param comparator
   */
  public NBest(int limit, Comparator<? super T> comparator) {
    super(limit, comparator);
    this.limit = limit;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * makes sure limit is not exceeded, NBest should contain the <limit> elements that are greatest
   */
  @Override
  public boolean add(T o) {
    return o == addRemove(o) ? false : true;
  }

  public T addRemove(T o) {
    super.add(o);
    T removed = null;
    if (size() > limit) {
      removed = poll();
    }
    return removed;
  }  
  
  /**
   * removes all elements and returns them in greatest to least order
   * 
   * @return list
   */
  public List<T> empty() {
    List<T> list = new ArrayList<T>();
    while (!isEmpty()) {
      list.add(remove());
    }
    Collections.reverse(list);
    return list;
  }
}
