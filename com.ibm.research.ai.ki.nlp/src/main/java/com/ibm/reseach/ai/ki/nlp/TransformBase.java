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
import java.util.regex.*;

import com.ibm.research.ai.ki.util.*;

public abstract class TransformBase implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * set by subclass
	 */
	protected Pattern toReplace;
	
	/**
	 * implemented by subclass
	 * m has matched the toReplace pattern. the text that was matched will be replaced by the text returned by this method
	 * @param m
	 * @return
	 */
	protected abstract String replacementText(Matcher m);
	
	/**
	 * Transforms the text, recording information needed to correct offsets from transformed to original and vice-versa
	 * @param text
	 * @param trans2orig constructed as new OffsetCorrection() (null if not desired)
	 * @param orig2trans constructed as new OffsetCorrection() (null if not desired)
	 * @return
	 */
	public String transform(String text, OffsetCorrection trans2orig, OffsetCorrection orig2trans) {
		Matcher m = toReplace.matcher(text);
		int prevEnd = 0;
		StringBuilder buf = new StringBuilder();
		while (m.find()) {
			buf.append(text.substring(prevEnd, m.start()));
			String replaceWith = replacementText(m);
			buf.append(replaceWith);
			if (m.end() - m.start() != replaceWith.length()) {
				if (trans2orig != null)
					trans2orig.addNextCorrectionPoint(buf.length(), m.end() - buf.length());
				if (orig2trans != null)
					orig2trans.addNextCorrectionPoint(m.end(), buf.length() - m.end());
			}
			prevEnd = m.end();
		}
		buf.append(text.substring(prevEnd));
		if (trans2orig != null)
			trans2orig.setOtherDocLength(text.length());
		if (orig2trans != null)
			orig2trans.setOtherDocLength(buf.length());
		return buf.toString();
	}
}
