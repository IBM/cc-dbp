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
package com.ibm.research.ai.ki.kbp.baselines;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.util.*;

/**
 * Convert our tsv format to the format used by NRE. Note though that the NRE format cannot support more relations between two terms than mentions.
 * @author mrglass
 *
 */
public class NREConvert {
	private void convert(File in, File outTsv) {
		PrintStream out = FileUtil.getFilePrintStream(outTsv.getAbsolutePath());
		for (File f : new FileUtil.FileIterable(in)) {
			for (List<RelexMention> ms : RelexMentionReader.getSetReader(f, RelexMention.class)) {
				RelexMention m0 = ms.get(0);
				List<String> rt = new ArrayList<String>(m0.relTypes);
				relTypes.addAll(rt);
				entities.add(m0.id1);
				entities.add(m0.id2);
				if (rt.size() > ms.size()) {
					System.err.println("Cannot convert for "+m0.id1+"  &&  "+m0.id2);
					continue;
					//throw new Error("cannot convert!");
				}
				for (int i = 0; i < ms.size(); ++i) {
					RelexMention m = ms.get(i);
					String rel = rt.isEmpty() ? "NA" : rt.get(i % rt.size());
					String sent = m.sentence.toLowerCase();
					String e1 = m.span1.substring(sent).replace(' ', '_');
					String e2 = m.span2.substring(sent).replace(' ', '_');
					
					if (m.span1.start < m.span2.start) {
						sent = sent.substring(0, m.span1.start) + e1 + sent.substring(m.span1.end, m.span2.start) + e2 + sent.substring(m.span2.end);
					} else {
						sent = sent.substring(0, m.span2.start) + e2 + sent.substring(m.span2.end, m.span1.start) + e1 + sent.substring(m.span1.end);
					}
					out.println(
							m.id1+"\t"+m.id2+"\t"+
							e1+"\t"+e2+"\t"+
							rel+"\t"+sent+"\t###END###");
					
				}
			}
		}
	}
	
	Set<String> relTypes = new HashSet<>();
	
	Set<String> entities = new HashSet<>();
	
	private void writeIds(Set<String> idSet, PrintStream out, char sep, int start) {
		List<String> ids = new ArrayList<>(idSet);
		Collections.sort(ids);
		for (int i = 0; i < ids.size(); ++i)
			out.println(ids.get(i)+sep+(i+start));
		out.close();
	}
	
	private void writeIdFiles(File outDir) {
		PrintStream out = FileUtil.getFilePrintStream(new File(outDir, "relation2id.txt").getAbsolutePath());
		out.println("NA 0");
		writeIds(relTypes, out, ' ', 1);
		writeIds(entities, FileUtil.getFilePrintStream(new File(outDir, "entity2id.txt").getAbsolutePath()), '\t', 0);
	}
	
	/**
	 * Example args:
	 simpleFormat/train.tsv
	 simpleFormat/test.tsv
	 idFiles/
	 * @param args
	 */
	public static void main(String[] args) {
		NREConvert con = new NREConvert();
		con.convert(new File(args[0]), new File(args[2], "train.txt"));
		con.convert(new File(args[1]), new File(args[2], "test.txt"));
		con.writeIdFiles(new File(args[2]));
	}
}
