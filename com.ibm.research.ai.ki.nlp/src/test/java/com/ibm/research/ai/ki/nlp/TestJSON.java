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
package com.ibm.research.ai.ki.nlp;

import java.util.*;
import java.util.function.*;
import java.util.regex.*;

import org.junit.*;
import org.junit.Assert.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class TestJSON {
	/**
	 * checks the basic stuff: Paragraph, Sentence, Token
	 * @param td
	 * @param tdjson
	 */
	protected void checkSame(Document td, Document tdjson) {
	    Author tdAuthor = td.getDocumentStructure(Author.class);
	    Author tdJsonAuthor = td.getDocumentStructure(Author.class);
	    Assert.assertEquals(tdAuthor == null, tdJsonAuthor == null);
	    if (tdAuthor != null)
	        Assert.assertEquals(tdAuthor.id, tdJsonAuthor.id);
		Assert.assertEquals(td.id, tdjson.id);
		Assert.assertEquals(td.text, tdjson.text);
		checkSame(td, tdjson, Paragraph.class, null);
		checkSame(td, tdjson, Sentence.class, null);
		checkSame(td, tdjson, Token.class, pp -> {
			Assert.assertEquals(pp.first.lemma, pp.second.lemma);
			Assert.assertEquals(pp.first.pos, pp.second.pos);
			Assert.assertEquals(pp.first.dependType, pp.second.dependType);
			Token d1 = pp.first.getDepends();
			Token d2 = pp.second.getDepends();
			Assert.assertTrue(d1 == null && d2 == null || d1 != null && d2 != null);
			if (d1 != null) {
				Assert.assertEquals(d1.start, d2.start);
				Assert.assertEquals(d1.end, d2.end);
			}
		});		
	}
	
	/**
	 * Only valid for annotations that have a total order (no same-span annotations)
	 * @param d1
	 * @param d2
	 * @param clz
	 * @param eqCheck
	 */
	protected <T extends Annotation> void checkSame(Document d1, Document d2, Class<T> clz, Consumer<Pair<T,T>> eqCheck) {
		List<T> a1 = d1.getAnnotations(clz);
		List<T> a2 = d2.getAnnotations(clz);
		Assert.assertEquals(a1.size(), a2.size());
		for (int i = 0; i < a1.size(); ++i) {
			T a1i = a1.get(i);
			T a2i = a2.get(i);
			Assert.assertEquals(a1i.source, a2i.source);
			Assert.assertEquals(a1i.start, a2i.start);
			Assert.assertEquals(a1i.end, a2i.end);
			if (eqCheck != null)
				eqCheck.accept(Pair.of(a1.get(i), a2.get(i)));
		}
		Collection<Class> d1s = d1.getDocumentStructuresPresent();
		Collection<Class> d2s = d2.getDocumentStructuresPresent();
		Assert.assertEquals(d1s.size(), d2s.size());
	}
	
	//TODO: check annotations with superclasses
	//TODO: check ListAnnotation - uses AnnoRef in somewhat complex way
	//TODO: check the rest of the model.types.*
	//TODO: implement and test DocumentStructures
	
	@Test
	public void checkSimple() {
		//just try Token, Sentence, Paragraph... the standard things
		Document td = new Document("test1", "This document is a good test for stuff.\n\nGood. The end.");
		td.addAnnotation(new Paragraph("TEST", 0, td.text.indexOf("\n\n")));
		td.addAnnotation(new Paragraph("TEST", td.text.indexOf("\n\n")+2, td.text.length()));
		int prevStart = 0;
		int period = -1;
		while ((period = td.text.indexOf('.', period+1)) != -1) {
			while (Character.isWhitespace(td.text.charAt(prevStart)))
				++prevStart;
			td.addAnnotation(new Sentence("TEST", prevStart, period));
			prevStart = period+1;	
		}
		Pattern tokBreak = Pattern.compile("\\W+");
		Matcher m = tokBreak.matcher(td.text);
		prevStart = 0;
		List<String> pos = Arrays.asList("NN", "VB", "IN");
		
		while (m.find()) {
			if (m.start() != prevStart) {
				Token t = new Token("TEST", prevStart, m.start());
				td.addAnnotation(t);
				t.pos = RandomUtil.randomMember(pos);
				t.lemma = t.coveredText(td).toLowerCase();
			}
			prevStart = m.end()+1;
		}
		
		//for each sentence, set the dependency tree to a DAG
		for (Sentence s : td.getAnnotations(Sentence.class)) {
			List<Token> toks = new ArrayList<>(td.getAnnotations(Token.class, s));
			Collections.shuffle(toks);
			for (int i = 0; i < toks.size()-1; ++i) {
				int depNdx = RandomUtil.randomInt(i+1, toks.size());
				toks.get(i).setDepends(td, toks.get(depNdx));
			}
		}
		
		td.setDocumentStructure(new Author("Jo Jo"));
		Categories cats = new Categories();
		cats.add("positive");
		cats.add("software");
		td.setDocumentStructure(cats);
		
		CorefIndex ci = CorefIndex.getCorefIndex(td);
		ci.addCoref(td.getAnnotations(Sentence.class));
		ci.addCoref(td.getAnnotations(Paragraph.class));
		
		String jstr = DocumentJSONSerializer.toJSON(td);
		System.out.println(jstr);
		Document tdjson = DocumentJSONDeserializer.fromJSON(jstr);
		checkSame(td, tdjson);
		System.out.println(DocumentJSONSerializer.toJSON(tdjson));
	}
}
