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
import java.util.logging.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

import edu.emory.clir.clearnlp.component.mode.morph.AbstractMPAnalyzer;
import edu.emory.clir.clearnlp.component.mode.pos.*;
import edu.emory.clir.clearnlp.component.utils.GlobalLexica;
import edu.emory.clir.clearnlp.component.utils.NLPUtils;
import edu.emory.clir.clearnlp.dependency.DEPTree;
import edu.emory.clir.clearnlp.util.lang.TLanguage;

public class ClearNLPPOS implements Annotator {
	private static final long serialVersionUID = 1L;
	private static Logger log = Logger.getLogger(ClearNLPPOS.class.getName());
	
	protected transient AbstractPOSTagger abstagger = null;
	protected transient AbstractMPAnalyzer lemmatizer = null;
	
	@Override
	public void initialize(final Properties config) {
		abstagger = NLPUtils.getPOSTagger(TLanguage.ENGLISH, "general-en-pos.xz");	
		lemmatizer = NLPUtils.getMPAnalyzer(TLanguage.ENGLISH);

		GlobalLexica.initDistributionalSemanticsWords(Arrays.asList(
				"brown-rcv1.clean.tokenized-CoNLL03.txt-c1000-freq1.txt.xz"));
	}

	@Override
	public void process(Document doc) {
		if (!doc.hasAnnotations(Sentence.class))
			Warnings.limitWarn(log, "ClearNLPPOS.missingSent", 3, "ClearNLPPOS: missing sentence annotations on "+doc.id);
		if (!doc.hasAnnotations(Token.class))
			Warnings.limitWarn(log, "ClearNLPPOS.missingTok", 3, "ClearNLPPOS: missing token annotations on "+doc.id);		

		for (Annotation sent : doc.getAnnotations(Sentence.class)) {
			List<Token> tokannos = doc.getAnnotations(Token.class, sent);
			if (tokannos.isEmpty()) {
				Warnings.limitWarn(log, "NOTOKENSINSENT", 10, "No tokens in sentence: "+doc.coveredText(sent));
				continue;
			}
			//put info into DEPTree representation
			List<String> tokens = doc.toStrings(tokannos);
			DEPTree tree = new DEPTree(tokens);
			
			//tag
			try {
				abstagger.process(tree);
				lemmatizer.process(tree);
			} catch (Throwable t) {
				boolean figuredOutProblem = false;
				for (Token tok : tokannos)
					if (tok.length() <= 0) {
						log.warning("Empty token. ClearNLPPOS doesn't like it!");
						figuredOutProblem = true;
						break;
					}
				if (!figuredOutProblem) {
					log.warning("Failure on "+doc.coveredText(sent));
					log.warning("Tokens:\n");
					for (Token tok : tokannos)
						log.warning("   "+doc.coveredText(tok));
				}
				Lang.error(t);
			}
			
			//add annotations from DEPTree
			for (int i=1; i < tree.size(); i++) {
				tokannos.get(i-1).pos = tree.get(i).getPOSTag();
				if (tree.get(i).getLemma() == null)
					Warnings.limitWarn(log, "NOLEMMA", 10, "ClearNLPPOS no lemma for "+doc.coveredText(tokannos.get(i-1)));
				tokannos.get(i-1).lemma = tree.get(i).getLemma();
			}
			
		}
	}

}
