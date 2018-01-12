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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.*;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.parallel.*;

public class Pipeline implements Annotator {
	private static final long serialVersionUID = 1L;
	
	protected List<Annotator> annotators;
	protected List<String> annotatorNames; //for directing configuration per-annotator
	
	//CONSIDER: enable profiling
	protected List<AtomicLong> nanos;
	
	public void enableProfiling() {
		nanos = new ArrayList<>(annotators.size());
		for (int i = 0; i < annotators.size(); ++i)
			nanos.add(new AtomicLong(0));
	}
	
	public long[] getMillisecondsPerAnnotator() {
		if (nanos == null || nanos.size() != annotators.size())
			throw new IllegalStateException("Profiling is not enabled");
		long[] millis = new long[nanos.size()];
		for (int i = 0; i < nanos.size(); ++i)
			millis[i] = nanos.get(i).get() / (long)1000000;
		return millis;
	}
	
	public String stringProfiling() {
		StringBuilder buf = new StringBuilder();
		long[] millis = getMillisecondsPerAnnotator();
		for (int i = 0; i < millis.length; ++i) {
			buf.append(annotatorName(i)+": "+Lang.milliStr(millis[i])+"\n");
		}
		return buf.toString();
	}
	
	public static final String allPrefix = "ALL.";
	public static Properties selectProperties(String forName, Properties allProps) {
		Properties selected = new Properties();
		String prefix = forName+".";
		for (String key : allProps.stringPropertyNames()) {
			if (key.startsWith(prefix))
				selected.setProperty(key.substring(prefix.length()), allProps.getProperty(key));
			else if (key.startsWith(allPrefix))
				selected.setProperty(key.substring(allPrefix.length()), allProps.getProperty(key));
		}
		return selected;
	}
	
	public Pipeline(Annotator... annotators) {
		this(Arrays.asList(annotators));
	}
	public Pipeline(List<Annotator> annotators) {
		this.annotators = new ArrayList<Annotator>(annotators);
	}
	@SafeVarargs
	public Pipeline(Pair<String,Annotator>... annotators) {
		this.annotators = new ArrayList<>();
		this.annotatorNames = new ArrayList<>();
		for (Pair<String,Annotator> p : annotators) {
			this.annotators.add(p.second);
			this.annotatorNames.add(p.first);
		}
	}

	/**
	 * Initialize all annotators in this pipeline with the Properties. 
	 * You can also pass the Pipeline pre-initialized Annotators. 
	 * Or use the constructor with named Annotators and use properties with 
	 * annotator_name.property_name syntax.
	 */
	@Override
	public void initialize(Properties config) {
		if (annotatorNames != null) {
			for (int ndx = 0; ndx < annotators.size(); ++ndx) {
				annotators.get(ndx).initialize(selectProperties(annotatorNames.get(ndx), config));
			}		
		} else {
			for (Annotator a : annotators)
				a.initialize(config);
		}
	}
	@Override
	public void process(Document doc) {
		if (nanos != null) {		
			for (int i = 0; i < annotators.size(); ++i) {
				long startTime = System.nanoTime();
				annotators.get(i).process(doc);
				nanos.get(i).addAndGet(System.nanoTime()-startTime);
			}
		} else {
			for (Annotator a : annotators)
				a.process(doc);
		}
	}
	
	public List<Annotator> getAnnotators() {
		return Collections.unmodifiableList(annotators);
	}
	
	protected String annotatorName(int ndx) {
		if (annotatorNames != null)
			return annotatorNames.get(ndx);
		return annotators.get(ndx).getClass().getSimpleName();
	}
	public String annotatorListing() {
		return annotatorListing(0);
	}
	protected String annotatorListing(int depth) {
		StringBuilder buf = new StringBuilder();
		for (int ndx = 0; ndx < annotators.size(); ++ndx) {
			Annotator a = annotators.get(ndx);
			if(a == null){
				System.out.println("null");
				continue;
			}
			if (a instanceof Pipeline) {
				buf.append(((Pipeline)a).annotatorListing(depth+1));			
			} else {
				for (int i = 0; i < depth; ++i)
					buf.append("  ");
				String profileStr = "";
		        if (nanos != null && nanos.size() == annotators.size()) {
		            profileStr = " ("+Lang.milliStr(nanos.get(ndx).get() / (long)1000000)+")";
		        }
				buf.append(annotatorName(ndx)+profileStr);
				buf.append('\n');
			}			
		}
		return buf.toString();
	}
	
	public Document process(String id, String text) {
		Document doc = new Document(id, text);
		process(doc);
		return doc;
	}
	
	public void loadProcessSave(File fromDir, File toDir) {
		loadProcessSave(fromDir, toDir, false, null);
	}
	
	public void loadProcessSave(File fromDir, File toDir, final boolean continueOnError) {
		loadProcessSave(fromDir, toDir, continueOnError, null);
	}
	
	public void loadProcessSave(Iterable<Document> fromDocs, File toDir) {
		loadProcessSave(fromDocs, toDir, false);
	}
	
	public void loadProcessSave(Iterable<Document> fromDocs, File toDir, final boolean continueOnError) {
		PeriodicChecker report = new PeriodicChecker(100);
		int docCount = 0;
		ISimpleExecutor threads = 
				//new SingleThreadedExecutor();
				new BlockingThreadedExecutor(2);
		try (DocumentWriter writer = new DocumentWriter(toDir)) {
			for (final Document doc : fromDocs) {
				++docCount;
				if (report.isTime()) {
					System.out.println("Pipeline.loadProcessSave: On document "+docCount);
				}
				threads.execute(new Runnable() {public void run() {
					if (continueOnError) {
						try {
							process(doc);
							synchronized (writer) {
								writer.write(doc);
							}
						} catch (Throwable t) {
							System.err.println("failure on doc "+doc.id+" skipping");
							t.printStackTrace();
						}
					} else {
						process(doc);
						synchronized (writer) {
							writer.write(doc);
						}
					}
				}});
			}
			threads.shutdown();
		}		
	}
	
	//NOTE: can re-order the documents; multithreaded
	public void loadProcessSave(File fromDir, File toDir, final boolean continueOnError, FileFilter fileFilter) {
		loadProcessSave(new DocumentReader(fromDir, fileFilter, false, null), toDir, continueOnError);	
	}
	

}
