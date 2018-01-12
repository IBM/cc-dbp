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

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;

import java.io.*;
import java.nio.file.*;
import java.util.zip.*;

/**
 * Version of DocumentWriter based on the new abstract class MultiFileWriter.
 * Needs testing before DocumentWriter is replaced.
 * @author mrglass
 *
 */
public class DocumentWriter2 extends MultiFileWriter<ObjectOutputStream, Document> {
	public DocumentWriter2(File rootDir) {
		super(rootDir);
	}

	public DocumentWriter2(File rootDir, int itemsPerFile, boolean overwrite) {
		super(rootDir, itemsPerFile, overwrite);
	}

	@Override
	protected String getExt() {
		return ".ser.gz";
	}

	@Override
	protected void write(ObjectOutputStream stream, Document obj) throws IOException {
		stream.writeObject(obj);
	}

	@Override
	protected ObjectOutputStream getStream(File f) throws IOException {
		return new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(f), 2 << 16));
	}


	public synchronized void write(Document doc) {	
		try {
			super.write(doc);
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	
	@Override
	protected void deepenDirectories() {
		super.deepenDirectories();
	}
	
	@Override
	public synchronized void close() {
		super.close();
	}
}
