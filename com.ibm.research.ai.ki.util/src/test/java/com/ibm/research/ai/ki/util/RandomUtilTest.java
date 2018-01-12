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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.research.ai.ki.util.*;

import org.junit.Test;

public class RandomUtilTest {

  @Test
  public void testRandomInt() {
    for (int i = 0; i < 100; i++) {
      assertEquals(0, RandomUtil.randomInt(0, 1));
    }
  }

  @Test
  public void testRandomMemberAndRemove() {
    Set<Integer> integers = new HashSet<Integer>();
    for (int i = 0; i < 10000; i++) {
      integers.add(i);
    }
    for (int i = 0; i < 10000; i++) {
      assertTrue(integers.contains(RandomUtil.randomMember(integers)));
    }
    
    for (int i = 0; i < 100000; i++) {
      assertTrue(!integers.contains(RandomUtil.removeRandom(integers)));
    }
  }

  @Test
  public void testRandomEntry() {
    Map<Integer, Integer> integers = new HashMap<Integer, Integer>();
    for (int i = 0; i < 10000; i++) {
      integers.put(i,i);
    }
    for (int i = 0; i < 10000; i++) {
      assertTrue(integers.containsKey(RandomUtil.randomEntry(integers).getKey()));
    }
  }
}
