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
import java.net.*;

import org.apache.commons.lang3.*;

import com.google.common.base.*;
import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Format for DBpedia type and property files (.nt)
 * @author mrglass
 *
 */
public abstract class NTriples {
	
	/**
	 * Order is arg1, relation, arg2
	 * @param filename
	 * @return
	 */
	public static Iterable<String[]> getProperties(File file) {
		return getProperties(file.getAbsolutePath());
	}
	/**
	 * Order is arg1, relation, arg2
	 * @param filename
	 * @return
	 */
	public static Iterable<String[]> getProperties(String filename) {
		return Iterables.transform(Iterables.filter(FileUtil.getLines(filename), noncomment),toPropertyTriple);
	}
	
	private NTriples() {}
	
    public static String getStringLiteral(String ntripleValue) {
        if (ntripleValue.indexOf("\"^^<http://www.w3.org/2001/XMLSchema") != -1)
            return StringEscapeUtils.unescapeJava(ntripleValue.substring(1, ntripleValue.indexOf("\"^^<http://www.w3.org/2001/XMLSchema")));
        if (!ntripleValue.startsWith("\""))
            throw new Error("bad format: "+ntripleValue);
        if (!ntripleValue.endsWith("\"") && (ntripleValue.lastIndexOf("\"@") == -1 || ntripleValue.lastIndexOf("\"@")+5 < ntripleValue.length()))
            throw new Error("bad format: "+ntripleValue);
        int endremove = ntripleValue.length()-1;
        if (ntripleValue.lastIndexOf("\"@")+5 >= ntripleValue.length())
            endremove = ntripleValue.lastIndexOf("\"@");
        return ntripleValue.substring(1, endremove);
    }
	

	static Predicate<String> noncomment = new Predicate<String>() {
		public boolean apply(String line) {
			return !line.startsWith("#");
		}	
	};
	
	//CONSIDER: support '@en' only filtering and transformation of ", including \" escapes
	//CONSIDER: support number values
	
	// {arg1, rel, arg2}
	static Function<String,String[]> toPropertyTriple = new Function<String,String[]>() {
		@Override
		public String[] apply(String line) {
		    try {
			if (!line.endsWith(" .")) {
				throw new Error("strange line: "+line);
			}
			int spndx = line.indexOf(' ');
			String arg1 = line.substring(0, spndx);
			int spndx2 = line.indexOf(' ', spndx+1);
			String rel = line.substring(spndx+1, spndx2);
			String arg2 = line.substring(spndx2+1, line.length()-2);
			
			if (!arg1.startsWith("<") || !arg1.endsWith(">"))
				throw new Error("strange arg1 "+arg1);
			arg1 = arg1.substring(1, arg1.length()-1);
			
			if (!rel.startsWith("<") || !rel.endsWith(">"))
				throw new Error("strange rel "+rel);				
			rel = rel.substring(1, rel.length()-1);
			
			if (arg2.startsWith("<") && arg2.endsWith(">"))
				arg2 = arg2.substring(1, arg2.length()-1);
			return new String[]{arg1, rel, arg2};
		    } catch (Exception e) {
		        throw new IllegalArgumentException("Bad line: "+line, e);
		    }
		}		
	};
}
