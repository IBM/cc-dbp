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
package com.ibm.research.ai.ki.kbp;

import java.io.*;
import java.util.*;

import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.RandomUtil.*;

/**
 * Reads the simple tsv format of RelexMention.Reader/Writer. And shows examples of 'interesting' entity-pair mention sets.
 * @author mrglass
 *
 */
public class ShowExamples {
	/**
	 * The samples that are interesting
	 * @param m
	 * @return
	 */
	static boolean isInteresting(List<RelexMention> m) {
		return m.size() > 1 && !m.get(0).isNegative();
	}
	
	/**
	 * Example args:
	 * simpleFormat/train.tsv
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		RandomUtil.Sample<String> sample = new RandomUtil.Sample<String>(20);
		for (List<RelexMention> m : RelexMentionReader.getSetReader(new File(args[0]), RelexMention.class)) {
			if (isInteresting(m) && sample.shouldSave()) {
				RelexMention m1 = m.get(0);
				sample.save(
						m1.span1.substring(m1.sentence)+"\t"+
						m1.span2.substring(m1.sentence)+"\t"+
						Lang.stringList(m1.relTypes, ",")+"\n  "+
						Lang.stringList(Iterables.transform(m, mi -> mi.sentence), "\n  "));
			}
		}

		System.out.println(Lang.stringList(sample, "\n\n=======================\n"));
	}
}
