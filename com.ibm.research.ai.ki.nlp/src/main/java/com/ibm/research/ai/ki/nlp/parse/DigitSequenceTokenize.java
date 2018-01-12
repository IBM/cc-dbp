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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Adds tokens for each digit sequence (\b[0-9]+\b)
 * @author mrglass
 *
 */
public class DigitSequenceTokenize implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String SOURCE = "DST";
	
	protected Pattern digitSeq = Pattern.compile("\\b[0-9]+\\b");
	
	@Override
	public void initialize(Properties config) {}

	@Override
	public void process(Document doc) {
		Matcher m = digitSeq.matcher(doc.text);
		NonOverlappingSpans nos = new NonOverlappingSpans();
		List<Token> toAdd = new ArrayList<>();
		while (m.find()) {
			Token t = new Token(SOURCE, m.start(), m.end());
			t.lemma = t.coveredText(doc);
			t.pos = "CD";
			if (!nos.addSpan(t)) {
				throw new Error("Span overlaps?? "+doc.toSimpleInlineMarkup());
			}
			toAdd.add(t);
		}
		//remove tokens that overlap with our new ones
		doc.removeAnnotations(Token.class, t -> nos.overlaps(t));
		for (Token t : toAdd)
			doc.addAnnotation(t);
	}

}
