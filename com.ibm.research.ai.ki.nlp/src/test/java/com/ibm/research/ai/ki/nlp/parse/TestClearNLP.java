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

import java.io.File;
import java.util.Properties;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

public class TestClearNLP {
	public static void main(String[] args) {
	    String testDir = args[0];
	    
		//ClearNLPTransform transform = new ClearNLPTransform();
		//System.out.println(transform.transform("The man ran (   ;  ) and so ( '  ).", null, null));
		//System.out.println(transform.transform("Alhazen\n\n(;   ), also known by the Lat", null, null));
		Pipeline p = new Pipeline(new ClearNLPSentence(), new ClearNLPPOS(), new ClearNLPParse());
		p.initialize(new Properties());
		p.enableProfiling();
		PeriodicChecker report = new PeriodicChecker(100);
		int docNum = 0;
		for (Document doc : new PipelinedDocuments(p, new DocumentReader(new File(testDir)))) {
			++docNum;
			if (report.isTime()) {
				System.out.println("On document "+docNum);
				System.out.println(p.stringProfiling());
			}
		}
		System.out.println(p.stringProfiling());
	}
}
