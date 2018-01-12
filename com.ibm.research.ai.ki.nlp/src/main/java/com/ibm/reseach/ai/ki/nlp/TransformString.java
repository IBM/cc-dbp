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

import java.util.*;
import java.util.regex.*;

import org.apache.commons.lang3.*;

import com.ibm.research.ai.ki.util.*;

public class TransformString extends TransformBase {
	private static final long serialVersionUID = 1L;
	
	protected Map<String,String> replacements;
	
	public TransformString(Map<String,String> replacements) {
		this.replacements = replacements;
		StringBuilder buf = new StringBuilder();
		for (Map.Entry<String,String> e : replacements.entrySet()) {
			buf.append("("+Pattern.quote(e.getKey())+")|");
		}
		buf.setLength(buf.length()-1);
		this.toReplace = Pattern.compile(buf.toString());
	}
	
	public TransformString(String mappingResourceTsv) {
		this(fromMappingResourceTsv(mappingResourceTsv));
	}
	
	/**
	 * loads the classpath resource. the resource is a tsv with java escaped strings [from] [tab] [to] on each line.
	 * @param mappingResourceTsv
	 * @return
	 */
	public static Map<String,String> fromMappingResourceTsv(String mappingResourceTsv) {
		String mappings = FileUtil.readResourceAsString(mappingResourceTsv);
		Map<String,String> replacements = new HashMap<>();
		for (String mapping : mappings.split("\n")) {
			if (mapping.trim().length() == 0)
				continue;
			String[] fromTo = mapping.split("\t");
			if (fromTo.length != 2)
				throw new IllegalArgumentException("bad line: "+mapping);
			replacements.put(StringEscapeUtils.unescapeJava(fromTo[0]), StringEscapeUtils.unescapeJava(fromTo[1]));
			//System.out.println(StringEscapeUtils.unescapeJava(fromTo[0]) + "->" + StringEscapeUtils.unescapeJava(fromTo[1]));

		}
		return replacements;
	}
	
	@Override
	protected String replacementText(Matcher m) {
		return replacements.get(m.group());
	}
	
}
