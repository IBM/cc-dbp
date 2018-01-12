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

import java.util.Arrays;

import com.ibm.research.ai.ki.util.*;

import org.junit.Test;

public class LangTest {
  @Test
  public void testRPADStringCharInt() {
    assertEquals("ThisI", Lang.RPAD("ThisIsAString", ' ', 5));
    assertEquals("ThisI", Lang.RPAD("ThisI", ' ', 5));
    assertEquals("This ", Lang.RPAD("This", ' ', 5));
    assertEquals("     ", Lang.RPAD("", ' ', 5));
    assertEquals(null, Lang.RPAD(null, ' ', 5));
    assertEquals("This#", Lang.RPAD("This", '#', 5));
  }

  @Test
  public void testLPADStringInt() {
    assertEquals("ThisI", Lang.LPAD("ThisIsAString", ' ', 5));
    assertEquals("ThisI", Lang.LPAD("ThisI", ' ', 5));
    assertEquals(" This", Lang.LPAD("This", ' ', 5));
    assertEquals("     ", Lang.LPAD("", ' ', 5));
    assertEquals(null, Lang.LPAD(null, ' ', 5));
    assertEquals("#This", Lang.LPAD("This", '#', 5));
  }

  @Test
  public void testTruncate() {
    assertEquals("ThisI", Lang.truncate("ThisIsAString", 5));
    assertEquals("ThisI", Lang.truncate("ThisI", 5));
    assertEquals("", Lang.truncate("", 5));
    assertEquals(null, Lang.truncate(null, 5));
  }

  @Test
  public void testNVL() {
    assertEquals("ThisIsAString", Lang.NVL("ThisIsAString", null));
    assertEquals(null, Lang.NVL(null, null));
    assertEquals("ThisIsAString", Lang.NVL("ThisIsAString", "ThisIsAnotherString"));
  }

  
  @Test
  public void testLinearSearch() {
    String[] animals = {"Dog", "Cat", "Mouse", "Dog"};
    assertEquals(1, Lang.linearSearch(animals, "Cat"));
    assertEquals(-1, Lang.linearSearch(animals, "NotAnAnimal"));
    assertEquals(0, Lang.linearSearch(animals, "Dog"));
    assertEquals(-1, Lang.linearSearch((Object[])null, "Dog"));
    assertEquals(-1, Lang.linearSearch(animals, null));
  }

  @Test
  public void testIsInteger() {
    assertEquals(true, Lang.isInteger("1"));
    assertEquals(false, Lang.isInteger("1.0"));
    assertEquals(false, Lang.isInteger("1f"));
    assertEquals(false, Lang.isInteger(null));
    assertEquals(true, Lang.isInteger(String.valueOf(Integer.MAX_VALUE)));
    assertEquals(true, Lang.isInteger(String.valueOf(Integer.MIN_VALUE)));
  }

  @Test
  public void testIsDouble() {
    assertEquals(true, Lang.isDouble("1"));
    assertEquals(true, Lang.isDouble("1.0"));
    assertEquals(true, Lang.isDouble("1f"));
    assertEquals(true, Lang.isDouble("1d"));
    assertEquals(false, Lang.isDouble(null));
    assertEquals(true, Lang.isDouble(String.valueOf(Double.MAX_VALUE)));
    assertEquals(true, Lang.isDouble(String.valueOf(Double.MIN_VALUE)));
  }

  @Test
  public void testStringListObjectArrayString() {
    String[] animals = {"Dog", "Cat", "Mouse", "Dog"};
    String[] nullArray = null;
    assertEquals("Dog|Cat|Mouse|Dog", Lang.stringList(animals,"|"));
    assertEquals("", Lang.stringList(new String[]{""},"|"));
    assertEquals(null, Lang.stringList(nullArray,"|"));
  }

  @Test
  public void testStringListIterableOfQString() {
    String[] animals = {"Dog", "Cat", "Mouse", "Dog"};
    assertEquals("Dog|Cat|Mouse|Dog", Lang.stringList(Arrays.asList(animals),"|"));
    assertEquals("", Lang.stringList(Arrays.asList(new String[]{""}),"|"));
  }

}
