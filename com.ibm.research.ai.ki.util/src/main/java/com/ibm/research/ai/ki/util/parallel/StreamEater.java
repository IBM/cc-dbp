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
package com.ibm.research.ai.ki.util.parallel;

import java.io.*;
import java.util.function.*;

import com.ibm.research.ai.ki.util.*;

public class StreamEater extends Thread {
	public static StreamEater eatStream(BufferedReader in, Consumer<String> lineHandler) {
		StreamEater e = new StreamEater(in, lineHandler);
		e.setDaemon(true);
		e.start();
		return e;
	}
	
	private StreamEater(BufferedReader in, Consumer<String> lineHandler) {
		this.in = in;
		this.lineHandler = lineHandler;
	}
	private BufferedReader in;
	private Consumer<String> lineHandler;
	@Override
	public void run() {
		try {
			String line = null;
			while ((line = in.readLine()) != null) {
				if (lineHandler != null)
					lineHandler.accept(line);
			}
			in.close();
		} catch (Exception e) {
			throw new Error(e);
		}		
	}
}