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

import opennlp.tools.sentdetect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class OpenNLPSentence implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String openNLPSentenceModel = "openNLPSentenceModel";
	public static final String defaultOpenNLPSentenceModel = "en-sent.bin";
	
	//protected transient SentenceDetectorME sentenceDetector;
	protected transient ThreadLocal<SentenceDetectorME> sentenceDetector;
	
	@Override
	public void initialize(Properties config) {
		try {
			String modelLocation = Lang.NVL(config.getProperty(openNLPSentenceModel), defaultOpenNLPSentenceModel);
			InputStream is = FileUtil.getResourceAsStream(modelLocation);
			if (is == null)
				throw new Error("cannot find "+modelLocation);
			final SentenceModel smodel = new SentenceModel(is);
			//sentenceDetector = new SentenceDetectorME(smodel);
			
			sentenceDetector = new ThreadLocal<SentenceDetectorME>() {
				@Override
				public SentenceDetectorME initialValue() {
					return new SentenceDetectorME(smodel);
				}
			};
			
			is.close();
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	@Override
	public void process(Document doc) {
		SentenceDetectorME sentenceDetect = sentenceDetector.get();
		for (Annotation segment : doc.getSegmentation(Paragraph.class)) {
			opennlp.tools.util.Span[] sentences = sentenceDetect.sentPosDetect(doc.coveredText(segment));
			for (opennlp.tools.util.Span sent : sentences) {
				int start = sent.getStart() + segment.start;
				int end = sent.getEnd() + segment.start;
				doc.addAnnotation(new Sentence(this.getClass().getSimpleName(), start, end));
			}
			//String[] detectedSentences = sentenceDetector.sentDetect(para.substring(doc.text));
		}
	}

}
