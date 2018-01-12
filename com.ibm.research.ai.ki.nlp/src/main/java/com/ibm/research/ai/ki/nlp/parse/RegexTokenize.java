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

import java.io.*;
import java.util.*;
import java.util.regex.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class RegexTokenize implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String REGEX_KEY = "tokenRegex";
	//whitespace or punctuation delimits tokens, and are themselves not tokens
	public static final String DEFAULT = "[\\p{Punct}\\s]+";
	
	public static final String WHITESPACE = Lang.pWhite_Space+"+";
	
	public static final String SOURCE = RegexTokenize.class.getSimpleName();
	
	protected Pattern tokenRegex;
	
	@Override
	public void initialize(Properties config) {
		tokenRegex =  Pattern.compile(Lang.NVL(config.getProperty(REGEX_KEY), DEFAULT));
	}

	@Override
	public void process(Document doc) {
		for (Annotation seg : doc.getSegmentation(Sentence.class, Paragraph.class)) {
			Matcher m = tokenRegex.matcher(doc.coveredText(seg));
			int prevStart = 0;
			while (m.find()) {
				if (m.start() > prevStart) {
					doc.addAnnotation(new Token(SOURCE, seg.start + prevStart, seg.start + m.start()));
				}
				prevStart = m.end();
			}
			if (prevStart != seg.length()) //may end with whitespace
				doc.addAnnotation(new Token(SOURCE, seg.start + prevStart, seg.end));
		}
	}

}
