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

import java.util.*;
import java.util.regex.*;

import com.google.common.collect.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;

public class RegexParagraph implements Annotator {
	private static final long serialVersionUID = 1L;
	
	public static final String REGEX_KEY = "paragraphRegex";
	//TODO: check that this matches exactly what "\\s*\\n+\\s*\\n+\\s*" matches
	public static final String BLANK_LINE="[ \\t\\x0B\\f\\r]*\\n[ \\t\\x0B\\f\\r]*\\n[ \\t\\x0B\\f\\r]*"; //at least two blank lines with any amount of extra whitespace
	
	protected Pattern paragraphRegex;
	
	@Override
	public void initialize(Properties config) {
		paragraphRegex =  Pattern.compile(Lang.NVL(config.getProperty(REGEX_KEY), BLANK_LINE));
		//System.out.println(paragraphRegex.toString());
	}

	
	
	@Override
	public void process(Document doc) {
		if (paragraphRegex == null)
			throw new IllegalArgumentException("Not initialized!");
		
		//do not add any paragraph annotations that overlap existing paragraph annotations
		//neither crossing nor containing nor contained
		RangeSet<Integer> unmarked = null;
		List<Paragraph> paras = doc.getAnnotations(Paragraph.class);
		if (!paras.isEmpty()) {
			RangeSet<Integer> existingPara = TreeRangeSet.create();
			for (Paragraph p : paras)
				existingPara.add(Range.closedOpen(p.start, p.end));
			unmarked = existingPara.complement();
		}
		
		Matcher m = paragraphRegex.matcher(doc.text);
		int prevEnd = 0;
		while (m.find()) {
			if (m.start() == prevEnd || doc.text.substring(prevEnd,m.start()).trim().isEmpty()) {
				prevEnd = m.end();
				continue;
			}
			if (unmarked == null || unmarked.encloses(Range.closedOpen(prevEnd, m.start())))
				doc.addAnnotation(new Paragraph(this.getClass().getName(), prevEnd, m.start()));
			prevEnd = m.end();
		}
		if (prevEnd < doc.text.length() &&
			(unmarked == null || unmarked.encloses(Range.closedOpen(prevEnd, doc.text.length())))) 
		{
			doc.addAnnotation(new Paragraph(this.getClass().getName(), prevEnd, doc.text.length()));
		}
	}

}
