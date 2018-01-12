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
import java.util.Properties;
import java.util.regex.*;

/**
 * Removes all annotations, or all annotations of the types specified.
 * @author mrglass
 *
 */
public class ResettingAnnotator implements Annotator {
	private static final long serialVersionUID = 1L;

	public static final String RESET_TYPES_KEY = "clearAnnotationTypes";
	public static final String RESET_TYPES_CLASS_SEPARATOR = ":";
	public static final String RESET_TYPES_DEFAULT = "";
	
	protected Class<? extends Annotation>[] typesToClear;
	
	@Override
	public void initialize(Properties config) {
		String types = config.getProperty(RESET_TYPES_KEY, RESET_TYPES_DEFAULT);
		if (!types.isEmpty()) {
			String[] classNames = types.split(Pattern.quote(RESET_TYPES_CLASS_SEPARATOR));
			typesToClear = new Class[classNames.length];			
			for (int i = 0; i < classNames.length; ++i) {
				try {
					Class c = Class.forName(classNames[i]);
					if (!Annotation.class.isAssignableFrom(c))
						throw new IllegalArgumentException("Not an Annotation: "+classNames[i]);
					typesToClear[i] = (Class<? extends Annotation>) c;
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Class not found: "+classNames[i]);
				}
			}			
		}
	}

	@Override
	public void process(Document doc) {
		//no types provided clears everything
		if (typesToClear == null)
			doc.clear();
		else
			for (Class<? extends Annotation> c : typesToClear)
				doc.removeAnnotations(c);
	}

}
