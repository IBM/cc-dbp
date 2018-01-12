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

import com.google.common.base.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import edu.emory.clir.clearnlp.component.utils.NLPUtils;
import edu.emory.clir.clearnlp.tokenization.AbstractTokenizer;
import edu.emory.clir.clearnlp.util.lang.TLanguage;

/**
 * Only use this if you are NOT also using clearNLP sentence segmentation
 * @author mrglass
 *
 */
public class ClearNLPTokenize implements Annotator {
	private static final long serialVersionUID = 1L;
	
	protected transient AbstractTokenizer tokenizer = null;
	protected transient ClearNLPTransform transform;
	
	private static final boolean debug = true;
	
	@Override
	public void initialize(Properties config) {
		tokenizer = NLPUtils.getTokenizer(TLanguage.ENGLISH);
		transform = new ClearNLPTransform();
	}

	@Override
	public void process(Document doc) {
		for (Annotation segment : doc.getSegmentation(Sentence.class, Paragraph.class)) {
			OffsetCorrection correct = new OffsetCorrection();
			String sentText = transform.transform(doc.coveredText(segment), correct, null);
			List<String> tokenList = tokenizer.tokenize(sentText);
			int curPos = 0;
			OverlappingSpans os = new OverlappingSpans();
			for (String token : tokenList) {
				int start = sentText.indexOf(token, curPos);
				if (start == -1)
					throw new Error("couldn't find span: "+token+" in "+sentText);
				int end = start + token.length();
				Token tok = new Token(this.getClass().getSimpleName(), 
						segment.start+correct.correct(start), 
						segment.start+correct.correct(end));
				if (tok.length() > 0) {
					doc.addAnnotation(tok);
					if (debug) {
						if (!os.getSpansOverlapping(tok).isEmpty()) {
							throw new IllegalStateException(
									"Produced overlapping spans for:\n"+doc.coveredText(segment)+
									"\n\nTOKENS\n   "+Lang.stringList(tokenList, "\n   "));
						}
						os.addSpan(tok);
					}
				}
				curPos = end;
			}
		}
	}

	
	public static void cleanEmptyTokens(Document doc) {
		doc.removeAnnotations(
				new Predicate<Annotation>() {
					@Override
					public boolean apply(Annotation input) {
						return input instanceof Token && input.length() == 0;
					}
			
				});
	}
}
