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
package com.ibm.research.ai.ki.formats;

import java.io.*;
import java.util.*;

import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;

public class SimpleTsvIterable implements Iterable<String[]> {
	protected String filename;
	protected boolean skipHeader;
	
	public SimpleTsvIterable(File file) {
		this(file.getAbsolutePath(),false);
	}
	public SimpleTsvIterable(File file, boolean skipHeader) {
		this(file.getAbsolutePath(),skipHeader);
	}
	
	public SimpleTsvIterable(String filename) {
		this(filename,false);
	}
	public SimpleTsvIterable(String filename, boolean skipHeader) {
		this.filename = filename;
		this.skipHeader = skipHeader;
	}	
	
	@Override
	public Iterator<String[]> iterator() {
		Iterator<String> lineIter = Iterators.filter(
				FileUtil.getRawLines(filename).iterator(), 
				s -> !s.isEmpty());
		if (skipHeader && lineIter.hasNext())
			lineIter.next();
		return new NextOnlyIterator<String[]>() {
			@Override
			protected String[] getNext() {
				if (!lineIter.hasNext())
					return null;
				return lineIter.next().split("\t");
			}
			
		};
	}


}
