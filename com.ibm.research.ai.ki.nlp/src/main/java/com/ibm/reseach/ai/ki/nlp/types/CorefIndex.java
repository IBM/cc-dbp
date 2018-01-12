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

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.*;
import com.google.common.base.*;
import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

public class CorefIndex implements DocumentStructure, Iterable<CorefIndex.Chain> {
	private static final long serialVersionUID = 1L;
	
	protected HashMap<AnnoRef<Annotation>, CorefIndex.Chain> index = new HashMap<>();
	//protected Set<CorefIndex.Chain> allChains = new HashSet<>();
	protected Document doc;
	
	private CorefIndex(Document doc) {
		this.doc = doc;
	};
	
	public void clear() {
		index.clear();
	}
	
	public static CorefIndex getCorefIndex(Document doc) {
		//CONSIDER: make this get-or-create in getDocumentStructure itself?
		CorefIndex ndx = doc.getDocumentStructure(CorefIndex.class);
		if (ndx == null) {
			ndx = new CorefIndex(doc);
			doc.setDocumentStructure(ndx);
		}
		return ndx;
	}

	public Chain newChain() {		
		Chain c = new Chain();
		addChain(c);
		return c;
	}
	
	public class Chain implements Iterable<Annotation>, Serializable {
		private static final long serialVersionUID = 1L;
		//private CorefIndex cindex;
		private HashSet<AnnoRef<Annotation>> chain = new HashSet<>();
			
		//Chain(CorefIndex index) {
		//	this.cindex = index;
		//}
		private boolean addInternal(Document doc, AnnoRef<Annotation> a) {
			return chain.add(a);
		}
		private boolean addInternal(Document doc, Annotation a) {
			return chain.add(doc.getAnnoRef(a));
		}
			
		public boolean add(Annotation a) {
			//if (cindex != null)
				return addCoref(this, a);
			//return addInternal(doc, a);
		}

		public int size() {
			return chain.size();
		}
		
		@Override
		public Iterator<Annotation> iterator() {
			return Iterators.transform(chain.iterator(), new Function<AnnoRef<Annotation>, Annotation>() {
				@Override
				public Annotation apply(AnnoRef<Annotation> input) {
					return input.get();
				}			
			});
		}
		
		public <T extends Annotation> T getFirst(Class<T> clazz) {
			T first = null;
			for (AnnoRef<Annotation> ar : chain) {
				Annotation a = ar.get();
				if (clazz.isInstance(a) && (first == null || first.start > a.start))
					first = (T)a;	
			}
			return first;
		}
		//CONSIDER: could put link to some KB entity too
		
		
	}
	
	/**
	 * null if there is no chain
	 * CONSIDER: return singleton chain?
	 * @param a
	 * @return
	 */
	public Chain getCorefChain(Annotation a) {
		return index.get(doc.getAnnoRef(a));
	}
	
	public Chain getOrCreateChain(Annotation a) {
		Chain c = index.get(doc.getAnnoRef(a));
		if (c == null) {
			c = newChain();
			c.add(a);
		}
		return c;
	}
	
	boolean addChain(CorefIndex.Chain chain) {
		boolean modified = false;
		for (AnnoRef<Annotation> a : chain.chain)
			modified |= (index.put(a, chain) != chain);
		//if (allChains == null) allChains = new HashSet<>();
		//allChains.add(chain);
		return modified;
	}
	
	/**
	 * Makes all of the annotations part of a single coref chain, if they are members of other chains, all of those chains are merged.
	 * @param as
	 * @return null if the annotations are empty, otherwise the resulting chain
	 */
	public Chain addCoref(Iterable<? extends Annotation> as) {
		Chain c = null;
		for (Annotation a : as) {
			if (c == null)
				c = getOrCreateChain(a);
			else
				c.addInternal(doc, a);
		}
		if (c != null)
			addChain(c);
		return c;
	}
	
	public boolean addCoref(Chain ca1, Annotation anno) {
		AnnoRef<Annotation> a2 = doc.getAnnoRef(anno);
		return addCoref(ca1, a2);
	}
	/**
	 * Makes a2 part of the chain ca1. 
	 * If a2 is a member of some other chain, that chain is merged into ca1.
	 * @param ca1
	 * @param a2
	 * @return
	 */
	protected boolean addCoref(Chain ca1, AnnoRef<Annotation> a2) {
		Chain ca2 = index.get(a2);
		if (ca1 == ca2)
			return false;
		if (ca2 == null) {
			index.put(a2, ca1);
			return ca1.addInternal(doc, a2);
		} else {
			for (Annotation a2i : ca2)
				ca1.addInternal(doc, a2i);
			return addChain(ca1);
		}
	}
	
	public void addCoref(Annotation anno1, Annotation anno2) {
		AnnoRef<Annotation> a1 = doc.getAnnoRef(anno1);
		AnnoRef<Annotation> a2 = doc.getAnnoRef(anno2);
		Chain ca1 = index.get(a1);
		if (ca1 != null) {
			addCoref(ca1, a2);
			return;
		}
		Chain ca2 = index.get(a2);
		if (ca2 != null) {
			addCoref(ca2, a1);
			return;
		}
		Chain c = new Chain();
		c.addInternal(doc, a1);
		c.addInternal(doc, a2);
		addChain(c);	
	}

	@Override
	public Iterator<Chain> iterator() {	
		Set<Chain> distinct = new HashSet<>();
		for (Chain c : index.values())
			distinct.add(c);
		return distinct.iterator();
		//return Iterators.unmodifiableIterator(Lang.NVL(allChains, Collections.EMPTY_SET).iterator());
	}
	
}
