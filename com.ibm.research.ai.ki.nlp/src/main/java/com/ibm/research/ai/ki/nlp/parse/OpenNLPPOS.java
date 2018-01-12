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

import opennlp.tools.postag.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class OpenNLPPOS implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String openNLPPOSModel = "openNLPPOSModel";
	public static final String defaultOpenNLPPOSModel = "en-pos-maxent.bin";
	
	protected transient ThreadLocal<POSTaggerME> posTagger;
	
	@Override
	public void initialize(Properties config) {
		try {
			String modelLocation = Lang.NVL(config.getProperty(openNLPPOSModel), defaultOpenNLPPOSModel);
			InputStream is = FileUtil.getResourceAsStream(modelLocation);
			if (is == null)
				throw new Error("cannot find "+modelLocation);
			final POSModel model = new POSModel(is);
			posTagger = new ThreadLocal<POSTaggerME>() {
				public POSTaggerME initialValue() {
					return new POSTaggerME(model);
				}
			};
			is.close();
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	@Override
	public void process(Document doc) {
		POSTaggerME tagger = posTagger.get();
		if (!doc.hasAnnotations(Sentence.class))
			Warnings.limitWarn("OpenNLPPOS.missingSent", 3, "missing sentence annotations on "+doc.id);
		if (!doc.hasAnnotations(Token.class))
			Warnings.limitWarn("OpenNLPPOS.missingTok", 3, "missing token annotations on "+doc.id);
		for (Annotation sent : doc.getAnnotations(Sentence.class)) {
			List<Token> tokannos = doc.getAnnotations(Token.class, sent);
			String[] tags = tagger.tag(doc.toStringArray(tokannos));
			for (int i = 0; i < tags.length; ++i)
				tokannos.get(i).pos = tags[i];
		}
	}

}
