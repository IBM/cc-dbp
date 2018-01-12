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

import java.io.*;
import java.util.*;
import java.util.stream.Stream;


/**
 * Idea is to track the points where the covering spans change
 * each such point gives the list of the spans that cover it.
 * Not really very fast actually, may improve this later.
 * @author mrglass
 *
 */
public class OverlappingSpans implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected List<Integer> coveredSpanPositions = new ArrayList<>(); //sorted
	protected List<Set<Span>> spansAtPosition = new ArrayList<>();
	
	public OverlappingSpans() {}
	
	/**
	 *
	 * @param spans need not be sorted
	 */
	public OverlappingSpans(Iterable<? extends Span> spans) {
		for (Span a : spans)
			this.addSpan(a);
	}
	
	public void addSpan(Span anno) {
		int sndx = findOrCreateSpanPoint(anno.start);
		int lndx = findOrCreateSpanPoint(anno.end);
		for (int i = sndx; i < lndx; ++i)
			spansAtPosition.get(i).add(anno);
	}
	
	protected int findOrCreateSpanPoint(int point) {
		int found = Collections.binarySearch(coveredSpanPositions, point);
		if (found < 0) {
			found = -(found+1);
			coveredSpanPositions.add(found, point);
			spansAtPosition.add(found, new HashSet<>());
		}
		return found;
	}
	
	protected int getPrecedingSpanNdx(int point) {
		//binary search on coveredSpanPositions
		int found = Collections.binarySearch(coveredSpanPositions, point);
		//return the Set<Span> that precedes it or equals it
		if (found < 0)
			found = -(found+1) - 1; //so found = (-(insertion point) - 1)
		return found;
	}
	
	public Set<Span> getSpansAt(int point) {
		int found = getPrecedingSpanNdx(point);
		if (found < 0)
			return Collections.EMPTY_SET; //point is before any annotations
		return spansAtPosition.get(found);
	}
	
	//public Stream<Span> getSpansCrossing(Span span) {
	//	return getSpansOverlapping(span).stream().filter(s -> span.crosses(s));
	//}
	
	public boolean hasOverlapping(Span span) {
		int found = getPrecedingSpanNdx(span.start);
		if (found < 0)
			++found;
		for (; found < coveredSpanPositions.size() && coveredSpanPositions.get(found) < span.end; ++found) {
			if (!spansAtPosition.get(found).isEmpty())
				return true;
		}
		return false;
	}
	
	public Set<Span> getSpansOverlapping(Span span) {
		Set<Span> merged = null;
		Set<Span> one = null;
		int found = getPrecedingSpanNdx(span.start);
		if (found < 0)
			++found;
		for (; found < coveredSpanPositions.size() && coveredSpanPositions.get(found) < span.end; ++found) {
			if (one != null) {
				if (merged == null) {
					merged = new HashSet<>();
					merged.addAll(one);
				}
				merged.addAll(spansAtPosition.get(found));
			} else {
				one = spansAtPosition.get(found);
			}
		}
		return Lang.NVL(merged, Lang.NVL(one, Collections.EMPTY_SET));
	}
}
