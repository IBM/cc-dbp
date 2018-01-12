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
import java.util.*;

import org.apache.commons.io.filefilter.*;

import com.google.common.collect.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Reads document collections created with DocumentWriter
 * assumes all documents are numeric files with .ser.gz extension
 * and contain serialized Document objects
 * @author mrglass
 *
 */
public class DocumentReader extends NestedIterable<File,Document> {

	public enum SerializedType {objectSerialized, plainText};
	
	public DocumentReader(File root) {
		this(root, null, false, null);
	}
	
	static FileFilter defaultFileFilter(SerializedType serializedType) {
		if (serializedType == null || serializedType == SerializedType.objectSerialized)
			return new RegexFileFilter("^[0-9]+\\.ser\\.gz$");
		else if (serializedType == SerializedType.plainText)
			return null;//new RegexFileFilter(".*\\.txt$");
		else
			throw new UnsupportedOperationException(serializedType.toString());
	}
	
	public DocumentReader(File root, FileFilter filter, boolean shuffle, SerializedType serializedType) {
		super(
			new FileUtil.FileIterable(
					root, 
					Lang.NVL(filter, defaultFileFilter(serializedType)), 
					shuffle), 
			file -> {
					if (serializedType == null || serializedType == SerializedType.objectSerialized)
						return new FileUtil.ObjectStreamIterator<>(file);
					else if (serializedType == SerializedType.plainText)
						return Iterators.singletonIterator(new Document(FileUtil.readFileAsString(file)));
					else
						throw new UnsupportedOperationException(serializedType.toString());
				});
		if (!root.exists())
			throw new IllegalArgumentException("No such file/directory: "+root);
	}

	/**
	 * Examines what type of annotations the document collection contains.
	 * @param args
	 */
	public static void main(String[] args) {
		int docCount = 0;
		int textLength = 0;
		Map<String,MutableDouble> annotationProfile = new HashMap<String,MutableDouble>();
		Map<String,MutableDouble> duplicateDocIds = new HashMap<>();
		for (Document doc : new DocumentReader(new File(args[0]))) {
			SparseVectors.increase(duplicateDocIds, doc.id, 1.0);
			++docCount;
			textLength += doc.text.length();
			for (Annotation a : doc.getAnnotations(Annotation.class)) {
				SparseVectors.increase(annotationProfile, a.getClass().getSimpleName(), 1.0);
				SparseVectors.increase(annotationProfile, a.getClass().getSimpleName()+"@"+a.source, 1.0);
			}
			if (doc.docLevelStructures != null)
				for (Map.Entry<Class, DocumentStructure> e : doc.docLevelStructures.entrySet()) {
					SparseVectors.increase(annotationProfile, e.getKey().getSimpleName(), 1.0);
				}
		}
		System.out.println("document count "+docCount+" avg text length "+(double)textLength/docCount);
		if (annotationProfile.size() == 0)
			System.out.println("no annotations");
		System.out.println(SparseVectors.toString(annotationProfile,100));
		SparseVectors.trimByThreshold(duplicateDocIds, 2.0);
		if (duplicateDocIds.isEmpty()) {
			System.out.println("no duplicate doc ids");
		} else {
			System.out.println("Duplicate doc ids:\n"+SparseVectors.toString(duplicateDocIds, 10));
			if (duplicateDocIds.size() > 10)
				System.out.println("and "+(duplicateDocIds.size() - 10)+" more duplicates");
		}
	}
	
	//TODO: also support direct reading - the PreprocessedIndex stuff
	
}
