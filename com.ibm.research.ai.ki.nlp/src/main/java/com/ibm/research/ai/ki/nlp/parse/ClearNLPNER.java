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

import edu.emory.clir.clearnlp.component.mode.ner.*;
import edu.emory.clir.clearnlp.component.utils.*;
import edu.emory.clir.clearnlp.dependency.*;
import edu.emory.clir.clearnlp.ner.*;
import edu.emory.clir.clearnlp.util.lang.*;

/**
 * Expects sentence, token, lemma and POS annotations to already exist.
 * Possible types are:
 * CARDINAL, ORG, DATE, PERSON, PERCENT, QUANTITY, GPE, PRODUCT, TIME, MONEY, NORP, ORDINAL, EVENT, WORK_OF_ART, LOC, FAC, LAW, LANGUAGE
 * @author mrglass
 *
 */
public class ClearNLPNER implements Annotator {
	private static final long serialVersionUID = 1L;

	protected transient AbstractNERecognizer nerTagger;
	
	static Logger log = Logger.getLogger(ClearNLPNER.class.getName());
	
	@Override
	public void initialize(Properties config) {
		GlobalLexica.initNamedEntityDictionary("general-en-ner-gazetteer.xz");
		nerTagger = NLPUtils.getNERecognizer(TLanguage.ENGLISH, "general-en-ner.xz");
	}

	@Override
	public void process(Document doc) {

		for (Annotation sent : doc.getAnnotations(Sentence.class)) {
			List<Token> tokannos = doc.getAnnotations(Token.class, sent);
			//NOTE: needs ClearNLPPOS (or substitute) run to get lemmas and POS
			
			//put info into DEPTree representation
			DEPTree tree = new DEPTree(doc.toStrings(tokannos));
			for (int i=1; i < tree.size(); i++) {
				tree.get(i).setPOSTag(tokannos.get(i-1).pos);
				if (tokannos.get(i-1).lemma == null)
					throw new Error("lemmas must be annotated for clearNLP NER - consider ClearNLPPOS");
				tree.get(i).setLemma(tokannos.get(i-1).lemma);
			}
			
			nerTagger.process(tree);
			
			//extract NEs
			String[] tags = tree.getNamedEntityTags();
			if (tags.length != tokannos.size()+1 || tags[0] != null)
				throw new IllegalStateException();
			int curTagStart = -1;
			String curTag = null;
			//CONSIDER: how to handle inconsistent tagging?
			//CONSIDER: pull into general tag to span library? (see also OpenNLPChunk)
			for (int i = 0; i < tokannos.size(); ++i) {
				BILOU n = NERLib.toBILOU(tags[i+1]);
				if (n == BILOU.B) {
					curTagStart = i; 
					curTag = tags[i+1].substring(2);
				} else if (n == BILOU.I) {
					if (curTagStart == -1 || curTag == null)
						Warnings.limitWarn(log, "badtagseq", 10, "bad tag seq: "+Arrays.toString(tags));
				} else if (n == BILOU.L) {
					doc.addAnnotation(new Entity(ClearNLPNER.class.getSimpleName(), tokannos.get(curTagStart).start, tokannos.get(i).end, curTag));
					curTag = null;
					curTagStart = -1;
				} else if (n == BILOU.O) {
					if (curTagStart != -1 || curTag != null) 
						Warnings.limitWarn(log, "badtagseq", 10, "bad tag seq: "+Arrays.toString(tags));
				} else if (n == BILOU.U) {
					if (curTagStart != -1 || curTag != null) 
						Warnings.limitWarn(log, "badtagseq", 10, "bad tag seq: "+Arrays.toString(tags));
					doc.addAnnotation(new Entity(ClearNLPNER.class.getSimpleName(), tokannos.get(i).start, tokannos.get(i).end, tags[i+1].substring(2))); 
				} else {
					throw new IllegalStateException();
				}
				//System.out.println(tokens.get(i)+": "+tags[i+1]);
			}
		}
	}

	public static void main(String[] args) {
		Document doc = new Document(
		        "IBM CEO Ginni Rometty discussed sales with Apple. Later things happened in Washington. "
		        + "Then Bob and Sam went to \"This Is The End\". "
		        + "IBM had a great year in 2012. Bob celebrated his birthday on June 7th, 2005.");
		Pipeline p = new Pipeline(new OpenNLPSentence(), new ClearNLPTokenize(), new ClearNLPPOS(), new ClearNLPNER());
		p.initialize(new Properties());
		p.process(doc);
		
		System.out.println(doc.toSimpleInlineMarkup());
	}
}
