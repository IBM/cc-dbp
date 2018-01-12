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
package com.ibm.research.ai.ki.nlp.parse;

import java.util.*;
import java.util.regex.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class EntityToOccurrences {
	
	private static final String beginRegex = "(^|\\W)";//"(^|\\s|[.!,?:\\(\\)\\[\\]\\{\\}]|[;-])";
	private static final String endRegex = "($|\\W)";//"($|\\s|[.!,'?:\\(\\)\\[\\]\\{\\}]|[;-])";
	
	/**
	 * Finds the spans in the document that correspond to the string 'entity'. Will not match in the middle of a word.
	 * @param doc
	 * @param entity
	 * @return
	 */
	public static List<Span> occurrencesExactIgnoreCase(Document doc, String entity) {
		Pattern p = Pattern.compile(beginRegex + Pattern.quote(entity) + endRegex, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(doc.text);
		List<Span> spans = new ArrayList<>();
		while (m.find()) {
			int start = m.start();
			if (start != 0)
			    ++start;
			int end = start + entity.length();
			spans.add(new Span(start, end));
		}
		return spans;
	}
	
	/**
	 * Remove spans from the list that cross sentences or do not match token boundaries.
	 * @param spans
	 * @param sentences may be null
	 * @param tokens may be null
	 */
	public static void discardBad(Iterable<? extends Span> spans, List<Sentence> sentences, List<Token> tokens) {
		Iterator<? extends Span> it = spans.iterator();
		while (it.hasNext()) {
			Span s = it.next();
			if (sentences != null && s.whichSegment(sentences) == -1) {
				it.remove(); //not contained fully in any sentence
			} else if (tokens != null) {
				Span ss = Span.toSegmentSpan(s, tokens);
				if (tokens.get(ss.start).start != s.start || tokens.get(ss.end-1).end != s.end)
					it.remove(); //not on token boundary
			}
		}
	}
	

}
