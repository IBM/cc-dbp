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

import com.ibm.research.ai.ki.util.*;

import org.junit.*;

import com.google.common.collect.*;

public class OverlappingSpansTest {
	private static class TSpan extends Span {
		public TSpan(int s, int e) {
			super(s,e);
		}
		public int hashCode() {
			return System.identityHashCode(this);
		}
		public boolean equals(Object o) {
			return this == o;
		}
	}
	
	@Test
	public void testVsLinearScan() {
		Random rand = new Random(123);
		
		for (int scale : new int[] {1, 8, 32}) {
			long sumOSTime = 0;
			long sumLSTime = 0;
			long sumOSTestTime = 0;
			long sumLSTestTime = 0;
			for (int testi = 0; testi < 100; ++testi) {
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
					spans.add(new TSpan(start, end));
				}
				
				//compare OverlappingSpans to linear scan
				OverlappingSpans os = new OverlappingSpans(spans);
				for (Span s : spans) {
					long start = System.nanoTime();
					Set<Span> overlapOS = os.getSpansOverlapping(s);
					sumOSTime += System.nanoTime() - start;
					
					start = System.nanoTime();
					Set<Span> overlapLS = new HashSet<>();
					for (Span ls : spans)
						if (ls.overlaps(s))
							overlapLS.add(ls);
					sumLSTime += System.nanoTime() - start;
					
					assertEquals(Sets.difference(overlapOS, overlapLS).size(), 0);
					
					start = System.nanoTime();
					boolean testOS = os.hasOverlapping(s);
					sumOSTestTime += System.nanoTime() - start;
					
					start = System.nanoTime();
					boolean testLS = false;
					for (Span ls : spans)
						if (ls.overlaps(s)) {
							testLS = true;
							break;
						}
					sumLSTestTime += System.nanoTime() - start;
					assertEquals(testOS, testLS);
				}
				
			}
			//System.out.println("Speedup = "+((double)sumLSTestTime)/((double)sumOSTestTime));
		}
	}
}
