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

import com.ibm.reseach.ai.ki.nlp.*;

/**
 * Like the text normalization in Google's w2v but with flag for [0-9] -> ' '
 * @author mrglass
 *
 */
public class NormalizeTextTransform extends TransformString {
	private static final long serialVersionUID = 1L;
	
	protected boolean removeDigits;
	
	public NormalizeTextTransform(boolean removeDigits) {
		super("com/ibm/research/ai/ki/nlp/parse/normalizeText-replace.tsv");
		this.removeDigits = removeDigits;
	}
	
	@Override
	public String transform(String text, OffsetCorrection trans2orig, OffsetCorrection orig2trans) {
		String result = super.transform(text,trans2orig,orig2trans).toLowerCase();
		if (removeDigits)
			result = result.replaceAll("[0-9]", " ");
		return result;
	}
}
