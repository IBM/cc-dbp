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

import java.util.*;

import org.junit.Test;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.OverlappingSpansTest.*;

public class SpanTest {

	public static List<Span> randomSpans(Random rand, int scale) {
		int doclen = rand.nextInt(100*scale) + 10*scale;
		int numspans = rand.nextInt(10*scale) + 1;
		List<Span> spans = new ArrayList<>();
		for (int si = 0; si < numspans; ++si) {
			int start = rand.nextInt(doclen-1);
			int end = 0;
			if (rand.nextBoolean()) {
				end = Math.min(doclen, start + rand.nextInt(10)+1);
			} else {
				end = Math.min(doclen, start + rand.nextInt(10*scale)+1);
			}
			spans.add(new Span(start, end));
		}
		return spans;
	}
	
  @Test
  public void testSubstring() {
    Span s = new Span(2, 9);
    assertEquals("cdedfgh", s.substring("abcdedfghijk"));
  }

  @Test
  public void testOverlapsAndContains() {
    Span s1 = new Span(3, 9);
    assertEquals(true, s1.overlaps(new Span(4, 9)));
    assertEquals(true, s1.overlaps(new Span(1, 4)));
    assertEquals(true, s1.overlaps(new Span(4, 6)));
    assertEquals(true, s1.contains(new Span(4, 6)));
    assertEquals(false, s1.overlaps(new Span(9, 15)));
    assertEquals(true, s1.overlaps(new Span(1, 4)));
  }

  @Test
  public void testToString() {
    Span s1 = new Span( 3 , 9 );
    assertEquals("[3,9)", s1.toString());
  }

  @Test
  public void testFromStringAndEquals() {
    assertEquals(new Span(4 , 9), Span.fromString("( 3 , 9 )"));
    assertEquals(new Span(3 , 9), Span.fromString("[3, 9 )"));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testFormat() {
    Span.fromString("(3,A)");
  }
}
