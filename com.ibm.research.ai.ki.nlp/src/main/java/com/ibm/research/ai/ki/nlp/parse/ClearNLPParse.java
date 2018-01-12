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
import java.util.zip.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import edu.emory.clir.clearnlp.component.mode.dep.*;
import edu.emory.clir.clearnlp.component.utils.NLPMode;
import edu.emory.clir.clearnlp.component.utils.NLPUtils;
import edu.emory.clir.clearnlp.dependency.DEPNode;
import edu.emory.clir.clearnlp.dependency.DEPTree;
import edu.emory.clir.clearnlp.reader.*;
import edu.emory.clir.clearnlp.util.lang.TLanguage;

public class ClearNLPParse implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String rootDependencyLabel = "root";
	
	public static final String clearNLPModelType = "clearNLPModelType";
	public static final String defaultClearNLPModelType = "general-en-dep.xz";
	public static final String maxSentenceLengthKey = "maxSentenceLengthToParse";
	
	protected transient AbstractDEPParser parser = null;
	protected int maxSentenceLength;
	
	@Override
	public void initialize(Properties config) {
		parser = NLPUtils.getDEPParser(
					TLanguage.ENGLISH, 
					config.getProperty(clearNLPModelType,defaultClearNLPModelType), 
					new DEPConfiguration(rootDependencyLabel));
	
		maxSentenceLength = Integer.parseInt(config.getProperty(maxSentenceLengthKey, "50"));
	}
	
	@Override
	public void process(Document doc) {
		for (Annotation sent : doc.getAnnotations(Sentence.class)) {
			List<Token> tokannos = doc.getAnnotations(Token.class, sent);
			//skip this one, it is probably not a real sentence and it will waste a lot of time to parse it
			if (tokannos.size() > maxSentenceLength)
				continue;
			//NOTE: needs ClearNLP POS run to get lemmas
			
			//put info into DEPTree representation
			List<String> tokens = doc.toStrings(tokannos);
			DEPTree tree = new DEPTree(tokens);
			for (int i=1; i < tree.size(); i++) {
				tree.get(i).setPOSTag(tokannos.get(i-1).pos);
				if (tokannos.get(i-1).lemma == null)
					throw new Error("lemmas must be annotated for clearNLP parsing - consider ClearNLPPOS");
				tree.get(i).setLemma(tokannos.get(i-1).lemma);
			}

			//parse
			parser.process(tree);
			
			//add annotations from DEPTree
			for (int i=1; i < tree.size(); i++) {
				DEPNode n = tree.get(i);
				//id is the one based index of the token n.id
				Token t = tokannos.get(i-1);
				int hid = n.getHead().getID();
				if (hid == 0) {
					t.setDepends(doc, null);
					t.dependType = rootDependencyLabel;
				} else {
					t.setDepends(doc, tokannos.get(hid-1));
					t.dependType = n.getLabel();
				}
			}
		}
	}

}
