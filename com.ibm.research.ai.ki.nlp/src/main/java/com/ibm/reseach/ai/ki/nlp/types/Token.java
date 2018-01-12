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
package com.ibm.reseach.ai.ki.nlp.types;

import java.util.*;

import com.fasterxml.jackson.annotation.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.graphs.*;

public class Token extends Annotation {
	
	private static final long serialVersionUID = 1L;
	
	@JsonCreator
	public Token(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end) {
		super(source, start, end);
	}

	public Token(Token t) {
		super(t.source, t.start, t.end);
		this.dependency = t.dependency;
		this.dependType = t.dependType;
		this.pos = t.pos;
		this.lemma = t.lemma;
	}
	
	/**
	 * Gets the head word (the lowest common ancestor in the dependency tree) for the set of tokens.
	 * Returns null if they are not in a tree together.
	 * @param toks
	 * @return
	 */
	public static Token getHead(Collection<Token> toks) {
		try {
			return TreeAlgorithms.getLCA(toks, t -> t.getDepends());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
	
	/**
	 * Starts with n1 and ends with n2. Goes from n1 up to the LCA, then from the LCA down to n2.
	 * Path should always be at least length 2 - the two tokens, but will be length 1 if they are the same.
	 * Returns null if there is no path (they are not in the same tree).
	 * @param n1
	 * @param n2
	 * @param getParent
	 * @return
	 */
	public static List<Token> getDependencyPath(Token t1, Token t2) {
		if (t1 == null || t2 == null)
			throw new IllegalArgumentException("Arguments cannot be null");
		try {
			return TreeAlgorithms.getPath(t1, t2, t -> t.getDepends());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
	
	/**
	 * Simple method to construct a Document, tokenizer.process it and fetch the coveredText of the tokens
	 * @param tokenizer
	 * @param text
	 * @return
	 */
	public static String[] tokenize(Annotator tokenizer, String text) {
	    Document doc = new Document(text);
	    tokenizer.process(doc);
	    return doc.toStringArray(doc.getAnnotations(Token.class));
	}
	
	/**
	 * part of speech tag
	 */
	public String pos;
	
	/**
	 * dependency parse parent or null
	 */
	protected AnnoRef<Token> dependency;
	protected AnnoRef<Token> getDependsRef() {
		return dependency;
	}
	protected void setDependsRef(AnnoRef<Token> dependency) {
		this.dependency = dependency;
	}
	public Token getDepends() {
		return dependency != null ? dependency.get() : null;
	}
	public void setDepends(Document doc, Token t) {
		if (t == null)
			dependency = null;
		else
			dependency = doc.getAnnoRef(t);
	}
	
	/**
	 * type of the dependency 'nn', 'amod', 'subj', 'root', etc
	 */
	public String dependType;
	
	/**
	 * lemma form of the token; could instead be index into dictionary?
	 */
	public String lemma;
}
