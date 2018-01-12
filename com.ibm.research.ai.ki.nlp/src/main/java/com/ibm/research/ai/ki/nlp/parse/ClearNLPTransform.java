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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Loads the clearNLP-replace as classpath resource. 
 * This transforms text into a form such that the output of clearNLP tokenization will match with the transformed text.
 * The OffsetCorrection produced by this class can then be used to find the spans in the original text.
 * @author mrglass
 *
 */
public class ClearNLPTransform extends TransformRegex {
	private static final long serialVersionUID = 1L;
	
	private static Map<String,String> getReplacementMap() {
		String mappings = FileUtil.readResourceAsString("com/ibm/research/ai/ki/nlp/parse/clearNLP-replace.tsv");
		Map<String,String> replacements = new LinkedHashMap<>();
		//must remove all whitespace after '(' and before ')'
		replacements.put("\\("+Lang.pWhite_Space+"+", "(");
		replacements.put(Lang.pWhite_Space+"+\\)", ")");
		//  and possibly for other brackets??
		//private final String[] L_BRACKETS = {"\"","(","{","["};
		//private final String[] R_BRACKETS = {"\"",")","}","]"};
		for (String mapping : mappings.split("\n")) {
			if (mapping.trim().length() == 0)
				continue;
			String[] fromTo = mapping.split("\t");
			if (fromTo.length != 2)
				throw new IllegalArgumentException("bad line: "+mapping);
			replacements.put(Pattern.quote(StringEscapeUtils.unescapeJava(fromTo[0])), StringEscapeUtils.unescapeJava(fromTo[1]));
		}
		return replacements;
	}
	
	public ClearNLPTransform() {
		//was just subclass of TransformString
		//super("com/ibm/sai/ie/parse/clearNLP-replace.tsv");
		super(getReplacementMap(), null);
	}

}
