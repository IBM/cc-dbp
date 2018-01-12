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

import opennlp.tools.tokenize.*;
import opennlp.tools.util.Span;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class OpenNLPTokenize implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String openNLPTokenModel = "openNLPTokenModel";
	//loaded from classpath
	public static final String defaultOpenNLPTokenModel = "en-token.bin";
	
	//protected transient Tokenizer tokenizer;
	protected transient ThreadLocal<Tokenizer> tokenizer;
	
	@Override
	public void initialize(Properties config) {
		try {
			String modelLocation = Lang.NVL(config.getProperty(openNLPTokenModel), defaultOpenNLPTokenModel);
			InputStream is = FileUtil.getResourceAsStream(modelLocation);
			if (is == null)
				throw new Error("cannot find "+modelLocation);
			final TokenizerModel tmodel = new TokenizerModel(is);
			//tokenizer = new TokenizerME(tmodel);
			
			tokenizer = new ThreadLocal<Tokenizer>() {
				public Tokenizer initialValue() {
					return new TokenizerME(tmodel);
				}
			};
			
			is.close();
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	@Override
	public void process(Document doc) {
		Tokenizer tokenize = tokenizer.get();
		for (Annotation segment : doc.getSegmentation(Sentence.class, Paragraph.class)) {
			Span tokenSpans[] = tokenize.tokenizePos(doc.coveredText(segment));
			for (Span s : tokenSpans) {
				doc.addAnnotation(new Token(this.getClass().getSimpleName(), 
						segment.start+s.getStart(), segment.start+s.getEnd()));
			}
		}
	}

}
