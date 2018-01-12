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

import org.junit.*;

import static org.junit.Assert.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

import java.util.*;

public class TestGazetteerMatcher {
	static final int numTest = 3;
	static final int numDocs = 10;
	//play with these to get better idea of practical speedup over different scales
	//mostly it is faster when the gazetteer is larger
	static final int vocabScale = 500;
	static final int gazScale = 1000;
	static final int docLenScale = 50;

	
	static class Comp implements Comparator<EntityWithId> {
		@Override
		public int compare(EntityWithId o1, EntityWithId o2) {
			int order = o1.compareTo(o2);
			if (order == 0)
				return o1.id.compareTo(o2.id);
			return order;
		}
		
	}
	
	public void check(List<EntityWithId> matches1, List<EntityWithId> matches2) {
		Collections.sort(matches1, new Comp());
		Collections.sort(matches2, new Comp());

		assertEquals(matches1.size(), matches2.size());
		for (int i = 0; i < matches1.size(); ++i) {
			EntityWithId m1 = matches1.get(i);
			EntityWithId m2 = matches2.get(i);
			if (m1.start != m2.start || m1.end != m2.end || !m1.id.equals(m2.id) || !m1.type.equals(m2.type))
				throw new Error();
		}
	}
	
	@Test
	public void verify() {
		long[] times = new long[2]; //baseline time and 'advanced' time
		for (int i = 0; i < numTest; ++i) {
			long[] t = verifyOne();
			times[0] += t[0];
			times[1] += t[1];
		}
		System.out.println("Speedup is "+(double)times[0]/times[1]);
	}
	
	public long[] verifyOne() {
		//select a token vocabulary
		Random rand = new Random();
		int vsize = rand.nextInt(10 * vocabScale) + vocabScale;
		String[] vocab = new String[vsize];
		for (int i = 0; i < vocab.length; ++i) {
			vocab[i] = RandomUtil.randomAlphaNumericString(rand.nextInt(10)+2, rand);
		}
		
		//build a set of terms from this
		int esize = rand.nextInt(10 * gazScale) + gazScale;
		Set<String> all = new HashSet<>();
		List<GazetteerMatcher.Entry> entries = new ArrayList<>();
		while (all.size() < esize) {
			int tlen = rand.nextInt(3) + 1;
			String[] toks = new String[tlen];
			for (int i = 0; i < toks.length; ++i)
				toks[i] = vocab[rand.nextInt(vocab.length)];
			String id = Lang.stringList(toks, " ");
			if (all.add(id)) {
				entries.add(new GazetteerMatcher.Entry(id, "type", toks, rand.nextBoolean()));
			}
		}
		
		Annotator tokenize = new RegexTokenize();
		tokenize.initialize(new Properties());
		GazetteerMatcher m = new GazetteerMatcher(entries);
		m.initialize(new Properties());
		
		long btime = 0;
		long atime = 0;
		//  build random documents
		//  verify that GazetteerMatcher.process and GazetterMatcher.baselineProcess give the same results
		for (int doci = 0; doci < numDocs; ++doci) {
			int dlen = rand.nextInt(10 * docLenScale) + docLenScale;
			StringBuilder buf = new StringBuilder();
			for (int ti = 0; ti < dlen; ++ti) {
				if (buf.length() > 0)
					buf.append(' ');
				if (rand.nextDouble() < 0.1) {
					buf.append(entries.get(rand.nextInt(entries.size())).id);
				} else {
					buf.append(vocab[rand.nextInt(vocab.length)]);
				}
			}
			
			Document doc = new Document(buf.toString());
			tokenize.process(doc);
			long startTime = System.nanoTime();
			m.process(doc);
			atime += System.nanoTime() - startTime;
			List<EntityWithId> matches1 = new ArrayList<>(doc.getAnnotations(EntityWithId.class));
			doc.removeAnnotations(EntityWithId.class);
			startTime = System.nanoTime();
			m.baselineProcess(doc);
			btime += System.nanoTime() - startTime;
			List<EntityWithId> matches2 = new ArrayList<>(doc.getAnnotations(EntityWithId.class));
			check(matches1, matches2);
		}
		return new long[] {btime, atime};
	}
}
