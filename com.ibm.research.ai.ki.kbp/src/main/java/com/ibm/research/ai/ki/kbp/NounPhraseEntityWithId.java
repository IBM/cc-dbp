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
package com.ibm.research.ai.ki.kbp;

import java.util.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;

public class NounPhraseEntityWithId implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String SOURCE = NounPhraseEntityWithId.class.getSimpleName();
	
	@Override
	public void initialize(Properties config) {}

	//NPs with these as their first tokens are not entity terms
	protected Set<String> ignoreFirstTokens = new HashSet<>(Arrays.asList(
			"the",
			"that", "these", "those", "this",
			"a", "an", 
			"who", "which", "it", 
			"its", "your", "our", "my", "their",
			"you", "me"));
	
	
	@Override
	public void process(Document doc) {
		for (Chunk c : doc.getAnnotations(Chunk.class)) {
			if ("NP".equals(c.tag)) {
				
				Token firstToken = doc.getAnnotations(Token.class, c).get(0);
				if (ignoreFirstTokens.contains(firstToken.coveredText(doc).toLowerCase()))
					continue;
				
				if (c.coveredText(doc).replaceAll("\\W+", "").isEmpty())
					continue;
				
				doc.addAnnotation(new EntityWithId(SOURCE, 
						c.start, c.end, 
						GroundTruth.unknownType, c.coveredText(doc).toLowerCase()));
			}
		}
		//we could drop chunk annotations now
	}

}
