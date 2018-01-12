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

import java.util.*;

/**
 * Not a big upgrade over a linear scan, we may improve this later.
 * @author mrglass
 *
 */
public class NonOverlappingSpans {
	protected List<Integer> starts = new ArrayList<>();
	protected List<Integer> ends = new ArrayList<>();
	
	/**
	 * Remove spans that overlap a longer span
	 * @param doc
	 * @param annos
	 * @return non-overlapping spans, sorted longest to shortest
	 */
	public static <T extends Span> List<T> longestSpansNoOverlap(Iterable<T> spans) {
		List<T> noOverlap = new ArrayList<>();
		for (T s : spans)
			noOverlap.add(s);
		//longest first sort
		Collections.sort(noOverlap, new Comparator<Span>() {
			@Override
			public int compare(Span o1, Span o2) {
				return o2.length() - o1.length();
			}	
		});
		
		//remove the ones that overlap
		NonOverlappingSpans nos = new NonOverlappingSpans();
		Iterator<T> it = noOverlap.iterator();
		while (it.hasNext()) {
			Span s = it.next();
			if (!nos.addSpan(s))
				it.remove();
		}
		return noOverlap;
	}
	
	public boolean overlaps(Span span) {
		if (span.length() == 0)
			return false;
		if (span.length() < 0)
			throw new IllegalArgumentException("bad span length: "+span);
		//CONSIDER: Arrays.binarySearch(a, key)
		int pos = Collections.binarySearch(starts, span.end-1); //find the start before our end
		if (pos >= 0)
			return true; //span that starts right as ours ends
		pos = -(pos+1)-1; //one before the insertion point, the largest start before our end
		if (pos >= 0 && ends.get(pos) > span.start) {
			//span that begins before our end and ends after our start
			return true;
		}
		return false;
	}
	
	/**
	 * Checks if the span can be added without overlapping an existing span. If it does not overlap, it is added.
	 * @param span
	 * @return true iff the span does not overlap any existing spans
	 */
	public boolean addSpan(Span span) {
		if (span.length() == 0)
			return true;
		if (span.length() < 0)
			throw new IllegalArgumentException("bad span length: "+span);
		//CONSIDER: Arrays.binarySearch(a, key)
		int pos = Collections.binarySearch(starts, span.end-1); //find the start before our end
		if (pos >= 0)
			return false; //span that starts right as ours ends
		pos = -(pos+1)-1; //one before the insertion point, the largest start before our end
		if (pos >= 0 && ends.get(pos) > span.start) {
			//span that begins before our end and ends after our start
			return false;
		}
		int startInsert = -(Collections.binarySearch(starts, span.start)+1);
		starts.add(startInsert, span.start);
		ends.add(startInsert, span.end);
		return true;
	}
}
