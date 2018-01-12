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
import java.util.Iterator;

public class Pair <T1, T2> implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  
  //two public members, first and second
  public T1 first;
  public T2 second;
  
  public Pair() {
  }

  public Pair(T1 first, T2 second) {
    super();
    this.first = first;
    this.second = second;
  }
  
  public T1 getFirst() {
    return first;
  }

  public void setFirst(T1 first) {
    this.first = first;
  }

  public T2 getSecond() {
    return second;
  }

  public void setSecond(T2 second) {
    this.second = second;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((first == null) ? 0 : first.hashCode());
    result = prime * result + ((second == null) ? 0 : second.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Pair)) {
      return false;
    }
    Pair other = (Pair) obj;
    if (first == null) {
      if (other.first != null) {
        return false;
      }
    } else if (!first.equals(other.first)) {
      return false;
    }
    if (second == null) {
      if (other.second != null) {
        return false;
      }
    } else if (!second.equals(other.second)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    return first.toString()+" "+second.toString();
  }
  
  public static <T1,T2> Pair<T1,T2> of(T1 f, T2 s) {
	  return new Pair<T1,T2>(f,s);
  }
  
  public static <S,T> Iterable<Pair<S,T>> zip(final Iterable<S> first, final Iterable<T> second) {
	  return new Iterable<Pair<S,T>>() {
		@Override
		public Iterator<Pair<S, T>> iterator() {		
			return new NextOnlyIterator<Pair<S,T>>() {
				Iterator<S> f = first.iterator();
				Iterator<T> s = second.iterator();
				@Override
				protected Pair<S, T> getNext() {
					if (!f.hasNext() && !s.hasNext())
						return null;
					if (!(f.hasNext() && s.hasNext()))
						throw new Error("first and second iterables are not the same length!");
					S fi = f.next();
					T si = s.next();
					return Pair.of(fi, si);
				}
				
			};
		}	  
	  };
  }
  
  public static <S,T> Iterable<Pair<S,T>> zip(final S[] first, final T[] second) {
	  if (first.length != second.length)
		  throw new Error("first and second arrays are not the same length!");
	  return new Iterable<Pair<S,T>>() {
		@Override
		public Iterator<Pair<S, T>> iterator() {		
			return new NextOnlyIterator<Pair<S,T>>() {
				int ii = 0;
				@Override
				protected Pair<S, T> getNext() {
					if (ii >= first.length)
						return null;
					S fi = first[ii];
					T si = second[ii];
					++ii;
					return Pair.of(fi, si);
				}		
			};
		}	  
	  };
  }
}
