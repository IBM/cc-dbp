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
package com.ibm.reseach.ai.ki.nlp;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Transform a document by matching regular expressions and replacing them with either a fixed string or a function of the matched string.
 * @author mrglass
 *
 */
public class TransformRegex extends TransformBase {
	private static final long serialVersionUID = 1L;
	
	protected String[] replacements;
	protected Function<String,String>[] replacementFunctions;
	
	/**
	 * NOTE: patterns are compiled as DOTALL and must not contain capturing groups!
	 * @param replace keys are regexes (may be null)
	 * @param replaceFunctions keys are regexes (may be null)
	 */
	public TransformRegex(Map<String,String> replace, Map<String, Function<String,String>> replaceFunctions) {
		StringBuilder toReplaceBuf = new StringBuilder();
		if (replace == null)
			replace = Collections.EMPTY_MAP;
		replacements = new String[replace.size()];
		int ndx = 0;
		for (Map.Entry<String, String> e : replace.entrySet()) {
			toReplaceBuf.append("("+e.getKey()+")|");
			replacements[ndx++] = e.getValue();
		}
		if (replaceFunctions == null)
			replaceFunctions = Collections.EMPTY_MAP;
		replacementFunctions = new Function[replaceFunctions.size()];
		ndx = 0;
		for (Map.Entry<String, Function<String,String>> e : replaceFunctions.entrySet()) {
			toReplaceBuf.append("("+e.getKey()+")|");
			replacementFunctions[ndx++] = e.getValue();
		}
		toReplaceBuf.setLength(toReplaceBuf.length()-1);
		toReplace = Pattern.compile(toReplaceBuf.toString(), Pattern.DOTALL);
	}
	
	protected String replacementText(int groupNdx, String orig) {
		--groupNdx; //convert to 0 based
		if (groupNdx < replacements.length)
			return replacements[groupNdx];
		return replacementFunctions[groupNdx-replacements.length].apply(orig);
	}
	
	protected String replacementText(Matcher m) {
		for (int gi = 1; gi <= m.groupCount(); ++gi) {
			String orig = m.group(gi);
			if (orig != null) {
				return replacementText(gi, orig);
			}
		}
		throw new Error("No group matched?!");
	}
	
	//TODO: move to unit test
	public static void main(String[] args) {
		Map<String,String> regexReplace = new LinkedHashMap<>();
		regexReplace.put("\\p{Punct}", "");
		regexReplace.put("(:?\\W*\\s+\\W*)+", " ");
		TransformRegex transform = new TransformRegex(regexReplace, null);
		OffsetCorrection orig2trans = new OffsetCorrection();
		OffsetCorrection trans2orig = new OffsetCorrection();
		String text = "the... text has        lots of ---- spaces               I.B.M. ";
		String ttext = transform.transform(text, trans2orig, orig2trans);
		Span s = new Span(55,text.length());
		Span t = new Span(10,20);
		System.out.println(text);
		System.out.println(ttext);
		System.out.println("Orig "+s);
		System.out.println(s.substring(text));
		orig2trans.correct(s);
		System.out.println(s);
		System.out.println(s.substring(ttext));
		
		System.out.println("Tras "+t);
		System.out.println(t.substring(ttext));
		trans2orig.correct(t);
		System.out.println(t);
		System.out.println(t.substring(text));
	}
}
