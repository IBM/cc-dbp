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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.ibm.research.ai.ki.util.*;

import org.junit.Test;

public class HashMapUtilTest {
  @Test
  public void testAddHS() {
    HashMap<TestK, HashSet<TestV>> map = new HashMap<TestK,HashSet<TestV>>();
    HashMapUtil.addHS(map, new TestK("k1"), new TestV("k1v1")) ;
    HashMapUtil.addHS(map, new TestK("k2"), new TestV("k2v2")) ;
    HashMapUtil.addHS(map, new TestK("k3"), new TestV("k3v3")) ;
    HashMapUtil.addHS(map, new TestK("k1"), new TestV("k1v2")) ;
    assertEquals(true, map.get(new TestK("k1")).contains(new TestV("k1v1")));
    assertEquals(true, map.get(new TestK("k1")).contains(new TestV("k1v2")));
    assertTrue(map.get(new TestK("k1")).size()==2);
    assertTrue(map.get(new TestK("k2")).size()==1);
  }

  @Test
  public void testAddALMapOfK1HashMapOfK2ArrayListOfVK1K2V() {
    HashMap<TestK1 ,HashMap<TestK2,ArrayList <TestV>>> map = new HashMap<TestK1 ,HashMap<TestK2,ArrayList <TestV>>>();
    HashMapUtil.addAL(map,new TestK1("k1"),new TestK2("k2"), new TestV("v1")) ;
    HashMapUtil.addAL(map,new TestK1("k1"),new TestK2("k2"), new TestV("v1")) ;
    HashMapUtil.addAL(map,new TestK1("k1"),new TestK2("k2"), new TestV("v2")) ;
    HashMapUtil.addAL(map,new TestK1("k1"),new TestK2("k3"), new TestV("v1")) ;
    HashMapUtil.addAL(map,new TestK1("k2"),new TestK2("k3"), new TestV("v1")) ;
    HashMapUtil.addAL(map,new TestK1("k3"),new TestK2("k3"), new TestV("v1")) ;
    assertEquals(2, map.get(new TestK1("k1")).keySet().size());
    assertEquals(3, map.get(new TestK1("k1")).get(new TestK2("k2")).size());
    assertTrue(map.get(new TestK1("k3")).get(new TestK2("k3")).contains(new TestV("v1")));
  }

  @Test
  public void testAddALMapOfKArrayListOfVKV() {
    Map<TestK,ArrayList<TestV>> map = new HashMap<TestK,ArrayList<TestV>>();
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v1"));
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v2"));
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v3"));
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v4"));
    HashMapUtil.addAL(map, new TestK("k2"), new TestV("v1"));
    HashMapUtil.addAL(map, new TestK("k2"), new TestV("v1"));
    assertEquals(4, map.get(new TestK("k1")).size());
    assertEquals(2, map.get(new TestK("k2")).size());
    assertTrue(map.get(new TestK("k1")).contains(new TestV("v4")));
    assertTrue(!map.get(new TestK("k2")).contains(new TestV("v4")));
  }

  @Test
  public void testPut2() {
    Map<TestK1, HashMap<TestK2, TestV>> map = new HashMap<HashMapUtilTest.TestK1, HashMap<TestK2,TestV>>();
    HashMapUtil.put2(map, new TestK1("k1"), new TestK2("k2"), new TestV("v1"));
    HashMapUtil.put2(map, new TestK1("k1"), new TestK2("k2"), new TestV("v3"));
    assertEquals(new TestV("v3"), map.get(new TestK1("k1")).get(new TestK2("k2")));
  }

  @Test
  public void testReverseHS() {
    HashMap<TestK, HashSet<TestV>> in = new HashMap<TestK, HashSet<TestV>>();
    HashMapUtil.addHS(in, new TestK("k1"), new TestV("A")) ;
    HashMapUtil.addHS(in, new TestK("k2"), new TestV("A")) ;
    HashMapUtil.addHS(in, new TestK("k3"), new TestV("B")) ;
    HashMapUtil.addHS(in, new TestK("k3"), new TestV("C")) ;
    
    for (Map.Entry<TestV, HashSet<TestK>> entry: HashMapUtil.reverseHS(in).entrySet()){
      Iterator<TestK> it = entry.getValue().iterator();
      while (it.hasNext()){
        TestK k = it.next();
        if (entry.getKey().getLabel().equalsIgnoreCase("C")){
          assertTrue(k.getLabel().equalsIgnoreCase("k3"));
        } 
        if (entry.getKey().getLabel().equalsIgnoreCase("B")){
          assertTrue(k.getLabel().equalsIgnoreCase("k3"));
        }
        if (entry.getKey().getLabel().equalsIgnoreCase("A")){
          assertTrue((k.getLabel().equalsIgnoreCase("k1"))||(k.getLabel().equalsIgnoreCase("k2")));
        }
      }
    }
    
  }

  @Test
  public void testReverseAL() {
    HashMap<TestK, ArrayList<TestV>> map = new HashMap<HashMapUtilTest.TestK, ArrayList<TestV>>();
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v1"));
    HashMapUtil.addAL(map, new TestK("v1"), new TestV("v1"));
    HashMapUtil.addAL(map, new TestK("k1"), new TestV("v1"));
    HashMapUtil.addAL(map, new TestK("k2"), new TestV("k1"));
    HashMapUtil.addAL(map, new TestK("k2"), new TestV("k1"));
    HashMapUtil.addAL(map, new TestK("v1"), new TestV("k2"));
    HashMapUtil.addAL(map, new TestK("k2"), new TestV("k2"));
    
    for (Map.Entry<TestV, ArrayList<TestK>> entry: HashMapUtil.reverseAL(map).entrySet()){
      Iterator<TestK> it = entry.getValue().iterator();
      while (it.hasNext()){
        TestK k = it.next();
        if (entry.getKey().getLabel().equalsIgnoreCase("k1")){
          assertTrue((k.getLabel().equalsIgnoreCase("k2")));
        } 
        if (entry.getKey().getLabel().equalsIgnoreCase("k2")){
          assertTrue((k.getLabel().equalsIgnoreCase("k2"))||(k.getLabel().equalsIgnoreCase("v1")));
        }
        if (entry.getKey().getLabel().equalsIgnoreCase("v1")){
          assertTrue((k.getLabel().equalsIgnoreCase("k1"))||(k.getLabel().equalsIgnoreCase("v1")));
        }
      }
    }
  }

  @Test
  public void testReverse() {
    HashMap<TestK, TestV> map = new HashMap<HashMapUtilTest.TestK, HashMapUtilTest.TestV>();
    map.put(new TestK("k1"), new TestV("v1"));
    map.put(new TestK("k1"), new TestV("v1"));
    map.put(new TestK("k1"), new TestV("v1"));
    map.put(new TestK("k1"), new TestV("v1"));
    map.put(new TestK("v1"), new TestV("k1"));
    map.put(new TestK("v1"), new TestV("k1"));
    map.put(new TestK("v1"), new TestV("k1"));
    for (Map.Entry<TestV, TestK> entry: HashMapUtil.reverse(map).entrySet()){
      if (entry.getKey().getLabel().equalsIgnoreCase("v1")){
        assertTrue((entry.getValue().getLabel().equalsIgnoreCase("k1")));
      }
      if (entry.getKey().getLabel().equalsIgnoreCase("k1")){
        assertTrue((entry.getValue().getLabel().equalsIgnoreCase("v1")));
      }
    }
  }
  
  abstract class Clazz{
    protected String label;
    protected abstract String getLabel();
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((label == null) ? 0 : label.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Clazz other = (Clazz) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (label == null) {
        if (other.label != null)
          return false;
      } else if (!label.equals(other.label))
        return false;
      return true;
    }
    private HashMapUtilTest getOuterType() {
      return HashMapUtilTest.this;
    }
  }
  
  class TestK extends Clazz{
    public TestK(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
  
  class TestV extends Clazz{
    public TestV(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
  
  class TestK1 extends Clazz{
    public TestK1(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
  
  class TestV1 extends Clazz{
    public TestV1(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
  
  class TestK2 extends Clazz{
    public TestK2(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
  
  class TestV2 extends Clazz{
    private String label;
    public TestV2(String label){
      this.label = label;
    }
    public String getLabel() {
      return label;
    }
  }
}
