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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Does simple matching of a Gazetteer against a Document, creating EntityWithId annotations.
 * The annotations may overlap. Requires tokenization.
 * 
 * CONSIDER: to match strings in text
  (https://github.com/TeamCohen/secondstring)
  SecondStringGazetteerMatcher
    IGazetteerMatcher
    
 * @author mrglass
 *
 */
public class GazetteerMatcher implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String SOURCE = GazetteerMatcher.class.getCanonicalName();
	
	public static List<Entry> buildSimpleEntries(Annotator tokenizer, Iterable<Pair<String,String>> termAndType, boolean caseInsensitive) {
		List<Entry> entries = new ArrayList<>();
		for (Pair<String,String> tt : termAndType) {
			Document doc = new Document(tt.first);
			tokenizer.process(doc);
			List<Token> tokens = doc.getAnnotations(Token.class);
			if (tokens.size() == 0)
				throw new Error("No tokens for "+tt.first);
			String[] tstrs = new String[tokens.size()];
			for (int i = 0; i < tstrs.length; ++i) {
				tstrs[i] = tokens.get(i).coveredText(doc);
			}
			Entry e = new Entry(tt.first, tt.second, tstrs);
			e.caseSensitive = !caseInsensitive;
			entries.add(e);
		}
		return entries;
	}
	
	public static class Entry implements Serializable {
		private static final long serialVersionUID = 1L;
		public Entry(String id, String type,  String[] tokens) {
			this.id = id;
			this.type = type;
			this.tokens = tokens;
		}
		public Entry(String id, String type,  String[] tokens, boolean caseSensitive) {
            this.id = id;
            this.type = type;
            this.tokens = tokens;
            this.caseSensitive = caseSensitive;
        }
		public String[] tokens;
		public String origLabel;
		public String id;
		public String type;
		
		//CONSIDER: could support an enum with different levels of normalization. 
		//GazetteerMatcher.normalize would have to use the highest level.
		public boolean caseSensitive; 
		
		public String toString() {
		    return id+"\t"+type+"\t"+Arrays.toString(tokens);
		}
		
		public boolean checkAndAdd(Document doc, List<Token> dtokens, int tokenStart) {
            if (tokens.length > dtokens.size()-tokenStart)
                return false;
		    if (caseSensitive) {
                for (int ti = 0; ti < tokens.length; ++ti) {
                    if (!dtokens.get(tokenStart+ti).coveredText(doc).equals(tokens[ti])) {
                        return false;
                    }
                }
            }
            doc.addAnnotation(new EntityWithId(
                        SOURCE, 
                        dtokens.get(tokenStart).start, 
                        dtokens.get(tokenStart+tokens.length-1).end,
                        type,
                        id));
            return true;
		}
	}

	interface ITokenMatcher {
		public void match(Document doc, List<Token> tokens, List<String> normToks, int tokenStart, int currentToken);
	}
	class HashTokenMatcher extends HashMap<String,ITokenMatcher> implements ITokenMatcher {
		private static final long serialVersionUID = 1L;

		//if normalized matching proceeds to the point, these entities have matched.
		List<GazetteerMatcher.Entry> exact = null;
		
		@Override
		public void match(Document doc, List<Token> tokens, List<String> normToks, int tokenStart, int currentToken) {
			if (exact != null) {
				for (GazetteerMatcher.Entry e : exact) {
				    e.checkAndAdd(doc, tokens, tokenStart);
				}
			}
			if (currentToken >= normToks.size())
				return;
			String tok = normToks.get(currentToken);
			ITokenMatcher m = this.get(tok);
			if (m != null) {
				m.match(doc, tokens, normToks, tokenStart, currentToken+1);
			}
		}
	}
	
	//not really tuned
	private static final int listMaxSize = 100;
	
	public ITokenMatcher build(Collection<Entry> entries) {
		ListTokenMatcher m = new ListTokenMatcher();
		m.addAll(entries);
		return m.split(0, listMaxSize);
	}
	
	class ListTokenMatcher extends ArrayList<Entry> implements ITokenMatcher {
		private static final long serialVersionUID = 1L;
		
		/**
		 * Split the token matcher to hash on the next token rather than linear scan through entities
		 * @param depth
		 * @param sizeLimit
		 * @return
		 */
		public ITokenMatcher split(int depth, int sizeLimit) {
			if (this.size() <= sizeLimit)
				return this;
			Map<String,ListTokenMatcher> tmpM = new HashMap<>();
			List<Entry> exact = null;
			for (Entry e : this) {
				if (e.tokens.length == depth) {
					if (exact == null)
						exact = new ArrayList<>();
					exact.add(e);
					continue;
				}
				String normTok = normalize(e.tokens[depth]);
				ListTokenMatcher l = tmpM.get(normTok);
				if (l == null) {
					l = new ListTokenMatcher();
					tmpM.put(normTok, l);
				}
				l.add(e);
			}
			HashTokenMatcher splitM = new HashTokenMatcher();
			splitM.exact = exact;
			for (Map.Entry<String, ListTokenMatcher> e : tmpM.entrySet()) {
				splitM.put(e.getKey(), e.getValue().split(depth+1, sizeLimit));
			}
			return splitM;
		}
		
		@Override
		/**
		 * check each Entry of this to see if (the rest of) it matches the tokens
		 * @param doc
		 * @param tokens
		 * @param normToks
		 * @param tokenStart the position in tokens (and normToks) where the matching was started
		 * @param currentToken the current position we are matching against in tokens (and normToks)
		 */
		public void match(Document doc, List<Token> tokens, List<String> normToks, int tokenStart, int currentToken) {
			int offset = currentToken - tokenStart;
			for (Entry e : this) {
				if (e.tokens.length > tokens.size() - tokenStart)
					continue;
				boolean matched = true;
				for (int i = 0; i < e.tokens.length-offset; ++i) {
                    if (!normToks.get(currentToken+i).equals(normalize(e.tokens[offset+i]))) {
                        matched = false;
                        break;
                    }
				}
				if (matched) {
				    e.checkAndAdd(doc, tokens, tokenStart);
				}
			}
		}
		
	}
	
	protected Collection<Entry> entries;
	protected ITokenMatcher matcher;
	
	//TODO: to support normalization that ignores non-word characters we would need to change the normToks to carry the offsets,
	//  since some tokens could be completely eliminated. tokens in gazetteer entries might also be reduced
	protected String normalize(String tokenString) {
		return tokenString.toLowerCase();
	}
	
	public GazetteerMatcher(Collection<Entry> entries) {
		//this.caseInsensitive = true;
		this.entries = entries;
		entries.removeIf(e -> e.tokens.length == 0);
		matcher = build(entries);
	}
	
	@Override
	public void initialize(Properties config) { 
		//CONSIDER: maybe do our matcher building here, and pass our flags for how the matching is done
	}

	@Override
	public void process(Document doc) {
		List<Token> tokens = doc.getAnnotations(Token.class);
		List<String> tstrs = new ArrayList<>();
		for (Token token : tokens)
			tstrs.add(normalize(token.coveredText(doc)));
		for (int i = 0; i < tokens.size(); ++i) {
			matcher.match(doc, tokens, tstrs, i, i);
		}
		//TODO: option to remove overlapping annotations? preferring longer spans
	}
	
	/**
	 * The simplest method. Mostly here to test that our hash based method works correctly.
	 * @param doc
	 */
	public void baselineProcess(Document doc) {
		List<Token> tokens = doc.getAnnotations(Token.class);
		List<String> tstrs = new ArrayList<>();
		for (Token token : tokens)
			tstrs.add(token.coveredText(doc));
		for (int i = 0; i < tokens.size(); ++i) {
			for (Entry e : entries) {
				if (e.tokens.length > tokens.size() - i)
					continue;
				boolean matched = true;
				for (int j = 0; j < e.tokens.length; ++j) {
				    if (e.caseSensitive ? !e.tokens[j].equals(tstrs.get(i+j)) : !e.tokens[j].equalsIgnoreCase(tstrs.get(i+j))) {
						matched = false;
						break;
					}
				}
				if (matched) {
					doc.addAnnotation(new EntityWithId(
							SOURCE, 
							tokens.get(i).start, 
							tokens.get(i+e.tokens.length-1).end,
							e.type,
							e.id));
				}
			}
		}
	}
}
