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

public class MutableInteger implements Cloneable, Comparable<MutableInteger>, Serializable {

  private static final long serialVersionUID = 1L;

  public int value;

  public MutableInteger() {
    value = 0;
  }

  public MutableInteger(int value) {
    this.value = value;
  }

  @Override
  public int compareTo(MutableInteger that) {
    return this.value == that.value ? 0 : this.value < that.value ? -1 : 1;
  }
  
  public String toString() {
	  return String.valueOf(value);
  }

}
