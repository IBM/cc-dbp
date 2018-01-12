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

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.ibm.research.ai.ki.util.*;

import org.junit.Test;

public class NBestTest {

  @Test
  public void testAddTAndEmpty() {
    int limit = 5;
    NBest<Person> nBest = new NBest<Person>(limit);
    int max = 0;
    for (int i=0; i< 20; i++){
      int rand = 1 + (int)(Math.random() * ((100 - 1) + 1));
      nBest.add(new Person("Name_"+i, rand));
      max = max<rand? rand:max;
    }
    List<Person> people = nBest.empty();
    assertEquals(limit, people.size());
    assertEquals(max, people.get(0).age);
    assertEquals(0, nBest.empty().size());
  }

  class Person implements Comparable<Person>{
    private String name;
    private int age;
    
    public Person(String name, int age) {
     this.name = name;
     this.age = age;
    }
    
    @Override
    public int compareTo(Person other) {
      if (this.age==other.age){
        return 0;
      }
      return this.age > other.age? 1:-1;
    }
    
    @Override
    public String toString() {
      return name+" : "+age;
    }
    
  }

}
