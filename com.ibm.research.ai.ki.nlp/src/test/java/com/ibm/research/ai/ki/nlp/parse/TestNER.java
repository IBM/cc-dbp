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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.RandomUtil.*;

import java.util.*;


public class TestNER {
	public static void main(String[] args) {
		File docDir = new File(args[0]);
		Pipeline p = new Pipeline(
				new ResettingAnnotator(), new OpenNLPSentence(), 
				new ClearNLPTokenize(), new ClearNLPPOS(), 
				new ClearNLPNER(), new OpenNLPNER());
		p.initialize(new Properties());
		p.enableProfiling();
		Iterable<Document> docs = new PipelinedDocuments(p, new DocumentReader(docDir));
		Map<String,MutableDouble> typeCounts = new HashMap<>();
		RandomUtil.Sample<String> sampled = new RandomUtil.Sample<>(50);
		for (Document doc : docs) {
			for (Entity e : doc.getAnnotations(Entity.class)) {
				SparseVectors.increase(typeCounts, e.type+"-"+e.source, 1.0);
				if (sampled.shouldSave())
					sampled.save(e.coveredText(doc)+" :: "+e.type+"-"+e.source);
			}
		}
		System.out.println(SparseVectors.toString(typeCounts));
		System.out.println(Lang.stringList(sampled, "\n"));
		System.out.println(p.annotatorListing());
	}
}
