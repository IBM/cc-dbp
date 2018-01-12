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
import java.util.*;


public class MutableDouble implements Cloneable, Comparable<MutableDouble>, Serializable {

  private static final long serialVersionUID = 1L;

  public double value;

  public MutableDouble() {
    value = 0;
  }

  public MutableDouble(double value) {
    this.value = value;
  }

  @Override
  public int compareTo(MutableDouble that) {
    return this.value == that.value ? 0 : this.value < that.value ? -1 : 1;
  }
  
  @Override
  public String toString() {
    return String.valueOf(this.value);
  }
  
	public static class AbsValueComparator implements Comparator<MutableDouble> {
		public int compare(MutableDouble o1, MutableDouble o2) {
			if (o1 == null || o2 == null) return 0;
			return (int)Math.signum(Math.abs(o1.value) - Math.abs(o2.value));
		}
		
	}  
}
