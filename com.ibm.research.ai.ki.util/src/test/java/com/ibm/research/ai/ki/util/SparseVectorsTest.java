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
import static org.junit.Assert.assertArrayEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.SparseVectors.*;

public class SparseVectorsTest {

  static HashMap<String, MutableDouble> map;

  static HashMap<String, HashMap<String, MutableDouble>> map2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    map = new HashMap<String, MutableDouble>();
    map.put("0.1", new MutableDouble(0.1));
    map.put("1.0", new MutableDouble(1.0));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.5", new MutableDouble(3.5));
    map.put("5.1", new MutableDouble(5.1));
    map.put("5.2", new MutableDouble(5.2));
    map.put("8.1", new MutableDouble(8.1));
    map.put("10.1", new MutableDouble(10.1));
    map2 = new HashMap<String, HashMap<String, MutableDouble>>();

    HashMap<String, MutableDouble> mapA = new HashMap<String, MutableDouble>();
    mapA.put("5.2", new MutableDouble(5.2));
    mapA.put("8.1", new MutableDouble(8.1));
    mapA.put("10.1", new MutableDouble(10.1));
    map2.put("A", mapA);

    HashMap<String, MutableDouble> mapB = new HashMap<String, MutableDouble>();
    mapB.put("4.1", new MutableDouble(4.1));
    map2.put("B", mapB);
  }

  @Test
  public void testGetMean() {
    assertEquals(SparseVectors.getMean(map), 4.4, 0.01);
  }

  @Test
  public void testGetVariance() {
    double mean = SparseVectors.getMean(map);
    assertEquals(SparseVectors.getVariance(map, mean), 10.4325, 0.001);
  }

  @Test
  public void testMaxKey() {
    assertEquals(SparseVectors.maxKey(map), "10.1");
  }

  @Test
  public void testNegativeDouble_maxKey() {
    HashMap<String, MutableDouble> map1 = new HashMap<String, MutableDouble>();
    map1.put("-0.1", new MutableDouble(-0.1));
    map1.put("-1.0", new MutableDouble(-1.0));
    map1.put("-10.1", new MutableDouble(-0.2));
    assertEquals(SparseVectors.maxKey(map1), "-0.1");
  }

  @Test
  public void testMaxValue() {
    assertEquals(SparseVectors.maxValue(map).value, 10.1, 0.001);
    HashMap<String, MutableDouble> mapE = new HashMap<String, MutableDouble>();
    mapE.put("-5.2", new MutableDouble(-5.2));
    mapE.put("-8.1", new MutableDouble(-8.1));
    mapE.put("-10.1", new MutableDouble(-10.1));
    assertEquals(SparseVectors.maxValue(mapE).value, -5.2, 0.001);
  }

  @Test
  public void testTrimByThreshold() {
	Map<String,MutableDouble> cm = Lang.deepCopy(map);
    SparseVectors.trimByThreshold(cm, 5.0);
    assertEquals(cm.size(), 4);
  }

  @Test
  public void testTrimDoubleByThreshold() {
    int count = SparseVectors.trimDoubleByThreshold(map2, 6.0);
    assertEquals(map2.get("A").size(), 2);
    assertEquals(map2.size(), 1);
    assertEquals(count, 2);
  }

  @Test
  public void testGetIncrease1() {
	Map<String,MutableDouble> cm = Lang.deepCopy(map);
    cm.put("10.5", new MutableDouble(10.5));
    assertEquals(false, SparseVectors.increase(cm, "10.5", 3.7));
    assertEquals(14.2, cm.get("10.5").value, 0);
    assertEquals(true, SparseVectors.increase(cm, "15.6", 3.7));
    assertEquals(3.7, cm.get("15.6").value, 0);
  }

  @Test
  public void testGetIncrease2() {
    HashMap<String, MutableDouble> mapC = new HashMap<String, MutableDouble>();
    assertEquals(true, SparseVectors.increase(mapC, "C", 16.1));
    assertEquals(16.1, mapC.get("C").value, 0);
    assertEquals(false, SparseVectors.increase(mapC, "C", 1));
    assertEquals(17.1, mapC.get("C").value, 0);
  }

  @Test
  public void testSum2() {
    Map<String, HashMap<String, MutableDouble>> outerMap = new HashMap<String, HashMap<String, MutableDouble>>();
    double total = 0d;
    for (int i = 0; i < 10; i++) {
      HashMap<String, MutableDouble> innerMap = new HashMap<String, MutableDouble>();
      for (int j = 0; j < 10; j++) {
        double val = Math.random();
        total += val;
        innerMap.put(i + "_" + j, new MutableDouble(val));
      }
      outerMap.put("" + i, innerMap);
    }
    assertEquals(total, SparseVectors.sum2(outerMap), 0.000001);
  }

  @Test
  public void testOneNorm() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    mapF.put("-1.0", new MutableDouble(-1.0));
    mapF.put("-2.0", new MutableDouble(-2.0));
    mapF.put("4.0", new MutableDouble(4.0));
    mapF.put("7.0", new MutableDouble(7.0));
    assertEquals(14.0, SparseVectors.oneNorm(mapF), 0);
  }

  @Test
  public void testTwoNorm() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    mapF.put("-3.0", new MutableDouble(-3.0));
    mapF.put("4.0", new MutableDouble(4.0));
    assertEquals(5.0, SparseVectors.twoNorm(mapF), 0);
  }

  @Test
  public void testScale() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    mapF.put("-3.0", new MutableDouble(-3.0));
    SparseVectors.scale(mapF, 5.0);
    assertEquals(-15.0, mapF.get("-3.0").value, 0);

    mapF.put("0.0", new MutableDouble(0.0));
    SparseVectors.scale(mapF, -5.0);
    assertEquals(0.0, mapF.get("0.0").value, 0);

    mapF.put("3.0", new MutableDouble(3.0));
    SparseVectors.scale(mapF, -5.0);
    assertEquals(-15.0, mapF.get("3.0").value, 0);

    mapF.put("-4.0", new MutableDouble(-4.0));
    SparseVectors.scale(mapF, -5.0);
    assertEquals(20.0, mapF.get("-4.0").value, 0);

  }

  @Test
  public void testNormalizeOne() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    mapF.put("-1.0", new MutableDouble(-1.0));
    mapF.put("-2.0", new MutableDouble(-2.0));
    mapF.put("4.0", new MutableDouble(4.0));
    mapF.put("7.0", new MutableDouble(7.0));
    assertEquals(14.0, SparseVectors.normalizeOne(mapF), 0);
    assertEquals(0.5, mapF.get("7.0").value, 0);
  }

  @Test
  public void testNormalize() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    mapF.put("3.0", new MutableDouble(3.0));
    mapF.put("4.0", new MutableDouble(4.0));
    assertEquals(5.0, SparseVectors.normalize(mapF), 0);
    assertEquals(0.8, mapF.get("4.0").value, 0);
  }

  @Test
  public void testDotProduct() {
    Map<String, MutableDouble> x = new HashMap<String, MutableDouble>();
    Map<String, MutableDouble> y = new HashMap<String, MutableDouble>();
    double dotProduct = 0d;
    for (int i = 0; i < 10; i++) {
      double val = Math.random();
      double va2 = Math.random();
      x.put("X_" + i, new MutableDouble(val));
      y.put("X_" + i, new MutableDouble(va2));
      dotProduct += val * va2;
    }
    assertEquals(dotProduct, SparseVectors.dotProduct(x, y), 0.0001);
  }

  @Test
  public void testGetDefaultZero() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("0.1", new MutableDouble(0.1));
    map.put("1.0", new MutableDouble(1.0));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.5", new MutableDouble(3.5));

    assertEquals(SparseVectors.getDefaultZero(map, "0.1"), 0.1, 0.001);
    assertEquals(SparseVectors.getDefaultZero(map, "0.10"), 0.0, 0.001);
  }

  @Test
  public void testAddTo() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("0.1", new MutableDouble(0.1));
    map.put("1.0", new MutableDouble(1.0));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.5", new MutableDouble(3.5));
    HashMap<String, MutableDouble> map_ = new HashMap<String, MutableDouble>();
    map_.put("0.11", new MutableDouble(0.11));
    map_.put("1.0", new MutableDouble(1.0));
    SparseVectors.addTo(map, map_);
    assertEquals(map.size(), 5);
  }

  @Test
  public void testSubtract() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("0.1", new MutableDouble(0.1));
    map.put("1.0", new MutableDouble(1.0));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.5", new MutableDouble(3.5));
    HashMap<String, MutableDouble> map_ = new HashMap<String, MutableDouble>();
    map_.put("0.11", new MutableDouble(0.11));
    map_.put("1.0", new MutableDouble(1.0));
    map_.put("2.1", new MutableDouble(2.1));
    SparseVectors.subtract(map, map_, false);
    SparseVectors.trimByThresholdAbs(map, 0.00001);
    assertEquals(map.size(), 2);
    HashMap<String, MutableDouble> __map = new HashMap<String, MutableDouble>();
    __map.put("0.1", new MutableDouble(0.1));
    __map.put("1.0", new MutableDouble(1.0));
    __map.put("2.1", new MutableDouble(2.1));
    __map.put("3.5", new MutableDouble(3.5));
    HashMap<String, MutableDouble> _map = new HashMap<String, MutableDouble>();
    _map.put("3.5", new MutableDouble(3.5));
    _map.put("0.3", new MutableDouble(0.3));
    _map.put("-1.03", new MutableDouble(-1.0));
    SparseVectors.subtract(__map, _map, true);
    SparseVectors.trimByThresholdAbs(map, 0.00001);
    assertEquals(__map.size(), 6);
    assertEquals(__map.get("-1.03").value, 1.0, 0.001);
    assertEquals(__map.get("0.3").value, -0.3, 0.001);
  }

  @Test
  public void testCosineSimilarity() {
    HashMap<String, MutableDouble> mapF = new HashMap<String, MutableDouble>();
    HashMap<String, MutableDouble> mapG = new HashMap<String, MutableDouble>();
    mapF.put("x_0", new MutableDouble(2.0));
    mapF.put("x_1", new MutableDouble(3.0));
    mapF.put("x_2", new MutableDouble(4.0));

    mapG.put("x_0", new MutableDouble(4.0));
    mapG.put("x_1", new MutableDouble(5.0));
    mapG.put("x_2", new MutableDouble(6.0));

    /*
     * Let mapF = 2 3 4 Let mapG = 4 5 6 Cosine Similarity (mapF, mapG) = dot(mapF, mapG) /
     * ||mapF||* ||mapG|| dot(mapF, mapG) = (2)*(4) + (3)*(5) + (4)*(6) = 47 ||mapF|| = sqrt((2)^2 +
     * (3)^2 + (4)^2) = 5.38516480713 ||mapG|| = sqrt((4)^2 + (5)^2 + (6)^2) = 8.77496438739 Cosine
     * Similarity (mapF, mapG) = 47 / (5.38516480713) * (8.77496438739) = 47 / 47.2546294028 =
     * 0.994611545873
     */
    assertEquals(0.9946115458726394, SparseVectors.cosineSimilarity(mapF, mapG), 0.0001);
  }

  @Test
  public void testGetHisto() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("1.1", new MutableDouble(1.1));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.1", new MutableDouble(3.1));
    map.put("4.1", new MutableDouble(4.1));
    map.put("5.1", new MutableDouble(5.1));
    map.put("6.1", new MutableDouble(6.1));
    map.put("6.5", new MutableDouble(6.5));
    map.put("6.7", new MutableDouble(6.7));
    map.put("7.0", new MutableDouble(7.0)); 
    map.put("7.5", new MutableDouble(7.5));
    map.put("7.6", new MutableDouble(7.6));
    map.put("8.5", new MutableDouble(8.5));
    map.put("8.9", new MutableDouble(8.5));
    double[] thres = new double[] { 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 }; // o yah use a for loop
    int[] res = SparseVectors.getHisto(map, thres);
    assertEquals(res.length, thres.length + 1);
    assertArrayEquals(new int[] { 1, 1, 1, 1, 1, 3, 3, 2 }, res);
    // System.out.println(SparseVectors.stringHisto(thres,res)); Just uncomment and review the
    // output. Too tedious to test
  }

  @Test
  public void testToString() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("1.1", new MutableDouble(1.1));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.1", new MutableDouble(3.1));
    map.put("4.1", new MutableDouble(4.1));
    map.put("5.1", new MutableDouble(5.1));
    map.put("6.1", new MutableDouble(6.1));
    map.put("6.5", new MutableDouble(6.5));
    map.put("6.7", new MutableDouble(6.7));
    map.put("7.0", new MutableDouble(7.0));
    map.put("7.5", new MutableDouble(7.5));
    map.put("7.6", new MutableDouble(7.6));
    map.put("-8.5", new MutableDouble(-8.5));
    map.put("8.9", new MutableDouble(8.9));
    String expected = " 8.9 8.9\n" + "-8.5 -8.5\n" + " 7.6 7.6\n" + " 7.5 7.5\n" + " 7.0 7.0\n"
            + " 6.7 6.7\n" + " 6.5 6.5\n" + " 6.1 6.1\n" + " 5.1 5.1\n" + " 4.1 4.1\n"
            + " 3.1 3.1\n" + " 2.1 2.1\n" + " 1.1 1.1\n";
    assertEquals(expected, SparseVectors.toString(map));
  }

  @Test
  public void testGetKeyDims() {
    HashMap<String, MutableDouble> map = new HashMap<String, MutableDouble>();
    map.put("1.1", new MutableDouble(1.1));
    map.put("2.1", new MutableDouble(2.1));
    map.put("3.1", new MutableDouble(3.1));
    map.put("4.1", new MutableDouble(4.1));
    map.put("5.1", new MutableDouble(5.1));
    map.put("6.1", new MutableDouble(6.1));
    map.put("6.5", new MutableDouble(6.5));
    map.put("6.7", new MutableDouble(6.7));
    map.put("7.0", new MutableDouble(7.0));
    map.put("7.5", new MutableDouble(7.5));
    map.put("7.6", new MutableDouble(7.6));
    map.put("8.5", new MutableDouble(8.5));
    map.put("8.9", new MutableDouble(8.9));
    assertArrayEquals(new String[] { "8.9", "8.5", "7.6" }, SparseVectors.getKeyDims(map, 3)
            .toArray());
  }

  @Test
  public void testGetOverlap() {
    HashMap<String, MutableDouble> m1 = new HashMap<String, MutableDouble>();
    m1.put("A", new MutableDouble(5.0));
    m1.put("B", new MutableDouble(4.0));
    m1.put("C", new MutableDouble(3.0));
    m1.put("D", new MutableDouble(-1.0));
    m1.put("E", new MutableDouble(-2.0));

    HashMap<String, MutableDouble> m2 = new HashMap<String, MutableDouble>();
    m2.put("A", new MutableDouble(7.0));
    m2.put("B", new MutableDouble(5.0));
    m2.put("C", new MutableDouble(0.0));
    m2.put("D", new MutableDouble(6.0));
    m2.put("K", new MutableDouble(-3.0));

    List<OverlapRecord<String>> list = SparseVectors.getOverlap(m1, m2);
    assertEquals(2, list.size());
  }

  @Test
  public void testConditionPMI() {
    class X {
      private int id;

      public X(int id) {
        this.id = id;
      }

      @Override
      public String toString() {
        return "X_" + id;
      }
    }

    class Y {
      private int id;

      public Y(int id) {
        this.id = id;
      }

      @Override
      public String toString() {
        return "Y_" + id;
      }
    }

    // Let us assume the following
    // total = 10;
    /**
     *  x  y   p(x, y) f(x,y)
        0   0   0.1     1      
        0   1   0.7     7
        1   0   0.15    1.5
        1   1   0.05    0.5
     * 
     *    p(x)  p(y)  f(x)  f(y)  
       0   .8  0.25    8    2.5 
       1   .2  0.75    2    7=.5
       
      pmi(x=0;y=0)  -1
      pmi(x=0;y=1)  0.222392421
      pmi(x=1;y=0)  1.584962501
      pmi(x=1;y=1)  -1.584962501
      
     */
    X x0 = new X(0);
    X x1 = new X(1);
    Y y0 = new Y(0);
    Y y1 = new Y(1);
    Map<X, MutableDouble> firstFreq = new LinkedHashMap<X, MutableDouble>();
    firstFreq.put(x0, new MutableDouble(8.0));
    firstFreq.put(x1, new MutableDouble(2.0));
    Map<Y, MutableDouble> secondFreq = new LinkedHashMap<Y, MutableDouble>();
    secondFreq.put(y0, new MutableDouble(2.5));
    secondFreq.put(y1, new MutableDouble(7.5));
    double firstTotal = 10;
    double secondTotal = 10;
    double coTotal = 20;
    LinkedHashMap<X, HashMap<Y, MutableDouble>> coOccurrence = new LinkedHashMap<X, HashMap<Y, MutableDouble>>();
    LinkedHashMap<Y, MutableDouble> x0Map = new LinkedHashMap<Y, MutableDouble>();
    x0Map.put(y0, new MutableDouble(0.1 * coTotal));
    x0Map.put(y1, new MutableDouble(0.7 * coTotal));
    LinkedHashMap<Y, MutableDouble> x1Map = new LinkedHashMap<Y, MutableDouble>();
    x1Map.put(y0, new MutableDouble(0.15 * coTotal));
    x1Map.put(y1, new MutableDouble(0.05 * coTotal));
    coOccurrence.put(x0, x0Map);
    coOccurrence.put(x1, x1Map);

    SparseVectors.conditionPMI(coOccurrence, firstFreq, secondFreq, firstTotal, secondTotal,
            coTotal);
    for (Map.Entry<X, HashMap<Y, MutableDouble>> outer : coOccurrence.entrySet()) {
      for (Map.Entry<Y, MutableDouble> inner : outer.getValue().entrySet()) {
        MutableDouble pmiTarget = inner.getValue();
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_0"))) {
          assertEquals(1.0, -pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_1"))) {
          assertEquals(0.222392421, pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_0"))) {
          assertEquals(1.584962501, pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_1"))) {
          assertEquals(1.584962501, -pmiTarget.value, 0);
        }
      }
    }
  }

  @Test
  public void testConditionPMI2() {
    class X {
      private int id;

      public X(int id) {
        this.id = id;
      }

      @Override
      public String toString() {
        return "X_" + id;
      }
    }

    class Y {
      private int id;

      public Y(int id) {
        this.id = id;
      }

      @Override
      public String toString() {
        return "Y_" + id;
      }
    }

    // Let us assume the following
    // total = 10;
    /**
     *  x  y   p(x, y) f(x,y)  p1(x, y) f1(x,y)  
        0   0   0.1     1      0.0     0  
        0   1   0.7     7      0.33    1 
        1   0   0.15    1.5    0.33    1
        1   1   0.05    0.5    0.33    1
     * 
     * p(x)  p(y)    f(x)  f(y)    p1(x) p1(y)    f1(x) f1(y)
       0   .8  0.25    8    2.5    0.33  0.33     1      1
       1   .2  0.75    2    7.5    0.66  0.66     2      2
       
      pmi(x=0;y=0)  -1
      pmi(x=0;y=1)  0.222392421
      pmi(x=1;y=0)  1.584962501
      pmi(x=1;y=1)  -1.584962501
      
      pmi1(x=0;y=0) = -infinity
      pmi1(x=0;y=1) = 0.599462...
      pmi1(x=1;y=0) = 0.599462...
      pmi1(x=1;y=1) = -0.400538...
    */

    X x0 = new X(0);
    X x1 = new X(1);
    Y y0 = new Y(0);
    Y y1 = new Y(1);
    double coTotal = 20;
    LinkedHashMap<X, HashMap<Y, MutableDouble>> coOccurrence = new LinkedHashMap<X, HashMap<Y, MutableDouble>>();
    LinkedHashMap<Y, MutableDouble> x0Map = new LinkedHashMap<Y, MutableDouble>();
    x0Map.put(y0, new MutableDouble(0.1 * coTotal));
    x0Map.put(y1, new MutableDouble(0.7 * coTotal));
    LinkedHashMap<Y, MutableDouble> x1Map = new LinkedHashMap<Y, MutableDouble>();
    x1Map.put(y0, new MutableDouble(0.15 * coTotal));
    x1Map.put(y1, new MutableDouble(0.05 * coTotal));
    coOccurrence.put(x0, x0Map);
    coOccurrence.put(x1, x1Map);

    SparseVectors.conditionPMI(coOccurrence);
    for (Map.Entry<X, HashMap<Y, MutableDouble>> outer : coOccurrence.entrySet()) {
      for (Map.Entry<Y, MutableDouble> inner : outer.getValue().entrySet()) {
        MutableDouble pmiTarget = inner.getValue();
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_0"))) {
          assertEquals(1.0, -pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_1"))) {
          assertEquals(0.222392421, pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_0"))) {
          assertEquals(1.584962501, pmiTarget.value, 0);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_1"))) {
          assertEquals(1.584962501, -pmiTarget.value, 0);
        }
      }
    }
    
    // Test 2
    /**
     *  x  y   p1(x, y) f1(x,y)  
        0   0   0.0     0  
        0   1   0.33    1 
        1   0   0.33    1
        1   1   0.33    1
     * 
     * value p1(x) p1(y)    f1(x) f1(y)
       0     0.33  0.33     1      1
       1     0.66  0.66     2      2
       
      pmi1(x=0;y=0) = -infinity
      pmi1(x=0;y=1) = 0.599462...
      pmi1(x=1;y=0) = 0.599462...
      pmi1(x=1;y=1) = -0.400538...
    */
    x0 = new X(0);
    x1 = new X(1);
    y0 = new Y(0);
    y1 = new Y(1);
    coOccurrence = new LinkedHashMap<X, HashMap<Y, MutableDouble>>();
    x0Map = new LinkedHashMap<Y, MutableDouble>();
    x0Map.put(y0, new MutableDouble(0.0 ));
    x0Map.put(y1, new MutableDouble(0.33));
    x1Map = new LinkedHashMap<Y, MutableDouble>();
    x1Map.put(y0, new MutableDouble(0.33 ));
    x1Map.put(y1, new MutableDouble(0.33 ));
    coOccurrence.put(x0, x0Map);
    coOccurrence.put(x1, x1Map);

    SparseVectors.conditionPMI(coOccurrence);
    for (Map.Entry<X, HashMap<Y, MutableDouble>> outer : coOccurrence.entrySet()) {
      for (Map.Entry<Y, MutableDouble> inner : outer.getValue().entrySet()) {
        MutableDouble pmiTarget = inner.getValue();
        System.out.println(outer.getKey()+" , "+inner.getKey()+" , "+pmiTarget.value);
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_0"))) {
          assertEquals(1.0, -pmiTarget.value, Double.NEGATIVE_INFINITY);
        }
        if ((outer.getKey()).equals("X_0") && (inner.getKey().equals("Y_1"))) {
          assertEquals(0.584962500721156, pmiTarget.value, 0.0001);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_0"))) {
          assertEquals(0.584962500721156, pmiTarget.value, 0.0001);
        }
        if ((outer.getKey()).equals("X_1") && (inner.getKey().equals("Y_1"))) {
          assertEquals(0.41503749927884404, -pmiTarget.value, 0.0001);
        }
      }
    }
   }
}
