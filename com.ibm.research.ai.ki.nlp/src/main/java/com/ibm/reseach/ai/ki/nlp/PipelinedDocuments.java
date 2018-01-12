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

import java.util.*;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.parallel.*;

public class PipelinedDocuments implements Iterable<Document> {

	public PipelinedDocuments(Annotator annotator, Iterable<Document> docs) {
		this(annotator, true, docs);
	}
	
	public PipelinedDocuments(Annotator annotator, boolean multiThreaded, Iterable<Document> docs) {
		this.annotator = annotator;
		this.docs = docs;
		this.multiThreaded = multiThreaded;
	}
	
	protected Annotator annotator;
	protected Iterable<Document> docs;
	protected boolean multiThreaded;
	
	protected PeriodicChecker report;
	
	public PipelinedDocuments setReportInterval(int numSecs) {
		report = new PeriodicChecker(numSecs);
		return this;
	}
	
	@Override
	public Iterator<Document> iterator() {
		final Iterator<Document> docit = docs.iterator();

		if (multiThreaded) {
			return new ThreadedLoopIterator<Document>(100) {
				@Override
				protected void loop() {
					int docCount = 0;
					BlockingThreadedExecutor threads = new BlockingThreadedExecutor(5);
					while (docit.hasNext()) {
						Document d = docit.next();
						++docCount;
						if (report != null && report.isTime()) {
							System.out.println("On document "+docCount+": "+d.id);
						}
						threads.execute(() -> {
							annotator.process(d);
							add(d);
						});
					}
					threads.awaitFinishing();
					if (report != null) {
						System.out.println("Time elapsed: "+Lang.milliStr(report.elapsedTime()));
					}
				}			
			};
		} else {
			return new Iterator<Document>() {
				int docCount = 0;
				@Override
				public boolean hasNext() {
					return docit.hasNext();
				}
	
				@Override
				public Document next() {
					Document d = docit.next();
					++docCount;
					if (report != null && report.isTime()) {
						System.out.println("On document "+docCount+": "+d.id);
					}
					annotator.process(d);
					return d;
				}
	
				@Override
				public void remove() {
					docit.remove();
				}		
			};
		}
	}

}
