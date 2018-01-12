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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import edu.emory.clir.clearnlp.component.utils.NLPUtils;
import edu.emory.clir.clearnlp.tokenization.AbstractTokenizer;
import edu.emory.clir.clearnlp.util.lang.TLanguage;

/**
 * Note that this does both sentence segmentation and tokenization
 * @author mrglass
 *
 */
public class ClearNLPSentence implements Annotator {
	private static final long serialVersionUID = 1L;
	
	protected transient AbstractTokenizer tokenizer;
	protected transient ClearNLPTransform transform;
	
	private static final boolean debug = true;
	private static final boolean skipBadTokens = true;
	
	@Override
	public void initialize(Properties config) {
		tokenizer = NLPUtils.getTokenizer(TLanguage.ENGLISH);
		transform = new ClearNLPTransform();
	}

	@Override
	public void process(Document doc) {
		OffsetCorrection correct = new OffsetCorrection();
		String ttext = transform.transform(doc.text, correct, null);
		List<List<String>> sentencesAndTokens = tokenizer.segmentize(new ByteArrayInputStream(ttext.getBytes()));
		int curPos = 0;
		OverlappingSpans os = new OverlappingSpans();
		for (List<String> sent : sentencesAndTokens) {
			int sentBegin = -1;
			int sentEnd = -1;
			for (int ti = 0; ti < sent.size(); ++ti) {
				int start = ttext.indexOf(sent.get(ti), curPos);
				if (start == -1) {
					if (skipBadTokens) {
						Span context = new Span(curPos-30, curPos+30);
						context.fix(0, ttext.length());
						System.err.println("Looking around for '"+sent.get(ti)+"', "
								+ "after "+(ti > 0 ? sent.get(ti-1):"*BEGIN*")
								+ " in text: '"+context.substring(ttext)+"'");
						correct.correct(context);
						System.err.println("Original was: "+context.substring(doc.text));
						continue;
					} else {
						throw new Error("couldn't find span: '"+sent.get(ti)+"' in:\n\n"+ttext);
					}
				} else if (debug && start > curPos+10 && ttext.substring(curPos, start).replaceAll(Lang.pWhite_Space+"+", "").length() > 8) {
					Warnings.limitWarn("SKIPALOTALIGN", 10, "Skipped over a lot to find token: '"+sent.get(ti)+"' in: "+
							ttext.substring(Math.max(0, curPos-30), Math.min(ttext.length(), start+30))+"\n\nSkipped segment:\n'"+
							ttext.substring(curPos, start).replaceAll(Lang.pWhite_Space+"+", "")+"'");
				}
				int end = start + sent.get(ti).length();
				Token tok = new Token("clearnlp", correct.correct(start), correct.correct(end));
				if (tok.length() > 0) {
					doc.addAnnotation(tok);
					if (debug) {
						if (!os.getSpansOverlapping(tok).isEmpty()) {
							throw new IllegalStateException(
									"Produced overlapping spans for:\n"+doc.coveredText(tok)+
									"\n\nTOKENS\n   "+Lang.stringList(os.getSpansOverlapping(tok), "\n   ")+
									"\n\nIN:\n   "+Lang.stringList(sent, "\n   "));
						}
						os.addSpan(tok);
					}
				}
				if (ti == 0)
					sentBegin = tok.start;
				if (ti == sent.size()-1)
					sentEnd = tok.end;
				curPos = end;
			}
			if (sentEnd > sentBegin)
				doc.addAnnotation(new Sentence(this.getClass().getSimpleName(), sentBegin, sentEnd));
		}
		
	}
	
}
