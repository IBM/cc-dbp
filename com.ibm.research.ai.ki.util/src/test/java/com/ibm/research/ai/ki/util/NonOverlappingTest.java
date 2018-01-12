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

public class NonOverlappingTest {
	@Test
	public void vsLinearScan() {
		Random rand = new Random(123);
		for (int scale : new int[] {1, 10, 100}) {
			long nosTime = 0;
			long lsTime = 0;
			for (int testi = 0; testi < 100; ++testi) {
				List<Span> spans = SpanTest.randomSpans(rand, scale);
				if (rand.nextBoolean()) {
					Collections.sort(spans, new Span.LengthComparator().reversed());
				}
				
				List<Span> nonOverlapping = new ArrayList<>();
				NonOverlappingSpans nos = new NonOverlappingSpans();
				for (Span s : spans) {
					long start = System.nanoTime();
					boolean lsOk = true;
					for (Span n : nonOverlapping)
						if (n.overlaps(s)) {
							lsOk = false;
							break;
						}
					if (lsOk)
						nonOverlapping.add(s);
					lsTime += System.nanoTime() - start;
					
					start = System.nanoTime();
					boolean nosOk = nos.addSpan(s);
					nosTime += System.nanoTime() - start;
					
					assertEquals(lsOk, nosOk);
				}
			}
			//System.out.println("Speedup = "+(double)lsTime/(double)nosTime);
		}
	}
}
