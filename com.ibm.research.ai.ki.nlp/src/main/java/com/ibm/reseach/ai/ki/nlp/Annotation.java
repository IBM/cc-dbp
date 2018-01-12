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
package com.ibm.reseach.ai.ki.nlp;

import java.util.*;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;

public class Annotation extends Span {
	private static final long serialVersionUID = 1L;
	
	/**
	 * usually name of analysis component that created the annotation
	 */
	public final String source;
	
	/**
	 * 
	 * @param source
	 * @param type
	 * @param first
	 * @param last
	 */
	@JsonCreator
	public Annotation(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end) {
		super(start, end);
		this.source = source;
	}

	public String coveredText(Document doc) {
		return this.substring(doc.text);
	}
	
	//NOTE: re-establish identity hashcode / equals
	
	@Override
	public int hashCode() {
		return System.identityHashCode(this);
	}
	
	@Override
	public boolean equals(Object obj) {
		return this == obj;
	}
	
	public boolean matchingSpan(Span s) {
		return this.start == s.start && this.end == s.end;
	}
	
	public String highlightLabel() {
		return this.getClass().getSimpleName();
	}
	public static final String HIGHLIGHT_BEGIN = "<<<";
	public static final String HIGHLIGHT_END = ">>>";

	public static <T extends Annotation> String highlightAll(String source, Iterable<T> spans) {
		return highlightAll(source, spans, 0);
	}
	/**
	 * NOTE: Doesn't handle overlapping spans (will just show the first breaking, ties by longest)
	 * @param source
	 * @param spans
	 * @param offset adjust all spans by this amount
	 * @return
	 */
	public static <T extends Annotation> String highlightAll(String source, Iterable<T> spans, int offset) {
		if (!Ordering.natural().isOrdered(spans)) {
			List<T> spanl = new ArrayList<T>();
			Iterables.addAll(spanl, spans);
			Collections.sort(spanl);
			spans = spanl;
		}
		StringBuffer buf = new StringBuffer();
		int leftOff = 0;
		for (T span : spans) {
			if (span.start+offset < 0 || span.end+offset >= source.length()) {
				continue;
			}
			if (span.start+offset >= leftOff) {
				try {
					buf.append(source.substring(leftOff,span.start+offset));
				} catch (Exception e) {
					System.out.println(source+" "+leftOff+", "+(span.start+offset));
				}
				buf.append(HIGHLIGHT_BEGIN);
				buf.append(span.highlightLabel());
				buf.append(":");
				buf.append(span.substring(source, offset));
				buf.append(HIGHLIGHT_END);
				leftOff = span.end+offset;
			}
		}
		buf.append(source.substring(leftOff));
		return buf.toString();
	}	
	
}
