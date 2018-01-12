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
import java.util.Properties;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

public abstract class Tokenizer {
	private static Annotator tokenizer = null;
	public static Annotator getTokenizer(RelexConfig config) {
		synchronized (Tokenizer.class) {
			if (tokenizer == null) {
			    if (config.tokenizerPipelineFile != null) {
			        tokenizer = FileUtil.loadObjectFromFile(config.tokenizerPipelineFile);
			    } else if (new File(config.convertDir, RelexDatasetFiles.tokenizerPipeline).exists()) {
			        tokenizer = FileUtil.loadObjectFromFile(new File(config.convertDir, RelexDatasetFiles.tokenizerPipeline));
			    } else {
    			    tokenizer = new Pipeline(
    				        new ClearNLPTokenize()
    				    //, new DigitSequenceTokenize() //add some special tokenization for digit groups
    				);
			    }
				tokenizer.initialize(new Properties());
			}
			return tokenizer;
		}
	}
}
