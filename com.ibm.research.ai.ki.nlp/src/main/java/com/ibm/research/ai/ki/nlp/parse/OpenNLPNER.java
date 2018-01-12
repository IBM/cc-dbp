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

import opennlp.tools.namefind.*;
import opennlp.tools.util.Span;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class OpenNLPNER implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String SOURCE = OpenNLPNER.class.getSimpleName();
	
	//types we have models for
	public static final String[] knownTypes = 
			new String[] {"Person", "Organization", "Location", "Date", "Time", "Percentage", "Money"};
	
	public OpenNLPNER() {
		this(knownTypes);
	}
	
	public OpenNLPNER(String... types) {
		this.types = types;
		for (String type : types)
			if (Lang.linearSearch(knownTypes, type) == -1)
				throw new IllegalArgumentException("Unknown type: "+type);
	}
	
	protected String[] types;
	
	//TODO: check if ThreadLocal is needed
	protected transient ThreadLocal<NameFinderME>[] nameFinder;
	
	@Override
	public void initialize(Properties config) {
		try {
			nameFinder = new ThreadLocal[types.length];
			for (int i = 0; i < types.length; ++i) {
				InputStream modelIn = FileUtil.getResourceAsStream("en-ner-"+types[i].toLowerCase()+".bin");
				TokenNameFinderModel model = new TokenNameFinderModel(modelIn);
				nameFinder[i] = new ThreadLocal<NameFinderME>() {
					@Override
					public NameFinderME initialValue() {
						return new NameFinderME(model);
					}
				};
			}
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	@Override
	public void process(Document doc) {
		for (Annotation segment : doc.getSegmentation(Sentence.class, Paragraph.class)) {
			List<Token> tokenAnnos = doc.getAnnotations(Token.class, segment);
			String[] tokens = doc.toStringArray(tokenAnnos);
			for (int ti = 0; ti < types.length; ++ti) {
				Span[] nameSpans = nameFinder[ti].get().find(tokens);
				for (Span ns : nameSpans) {
					int charStart = tokenAnnos.get(ns.getStart()).start;
					int charEnd = tokenAnnos.get(ns.getEnd()-1).end;
					doc.addAnnotation(new Entity(SOURCE, charStart, charEnd, types[ti]));
				}
			}
		}
	}

}
