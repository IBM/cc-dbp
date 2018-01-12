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
package com.ibm.research.ai.ki.kbp.embeddings;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import com.google.common.io.LittleEndianDataInputStream;

import com.ibm.research.ai.ki.util.*;

/**
 * Converts the google word2vec format to EmbeddingFormat format.
 * @author mrglass
 *
 */
public class Word2VecConverter {
	int dims;
	int numItems;
	Iterator<Pair<String,float[]>> iter;
	
	public static final String defaultEncoding = "UTF-8";
	
	private static String readText(InputStream in, char end, String encoding) throws Exception {
		int c;
		List<Integer> read = new ArrayList<>();
		while ((c = in.read()) != end) {
			if (c != '\n') //may or may not have a leading newline in google word2vec binary format
				read.add(c);
		}
		byte[] strBytes = new byte[read.size()];
		for (int i = 0; i < strBytes.length; ++i)
			strBytes[i] = (byte)(int)read.get(i);
		return new String(strBytes, encoding);
	}
	
	//also work with Google's text format
	public void initText(String filename) {
		Iterator<String> lineIt = FileUtil.getRawLines(filename).iterator();
		String[] header = lineIt.next().split("\\s+");
		this.numItems = Integer.parseInt(header[0]);
		this.dims = Integer.parseInt(header[1]);
		System.out.println("Loading "+numItems+" with "+dims+" dimensions");
		this.iter = new ThreadedLoopIterator<Pair<String,float[]>>() {
			@Override
			protected void loop() {
				while (lineIt.hasNext()) {
					String line = lineIt.next();
					String[] parts = line.split("\\s+");
					if (parts.length != 1+dims) {
						throw new Error("bad line: "+line);
					}
				
					String word = parts[0].replace('_', ' ');
					float[] vector = new float[dims];
					for (int i = 1; i < parts.length; ++i)
						vector[i-1] = Float.parseFloat(parts[i]);
					add(Pair.of(word, vector));
				}
			}
		};
	}
	
	/**
	 * Google's word2vec binary format:
	 * numberOfWordsAsAsciiInteger [space] numberOfDimensionsAsAsciiInteger [newline]
	 * <for numberOfWords:>
	 *   wordAsUTF8 [space] 
	 *   <for numberOfDimensions:>
	 *   	floatInLittleEndian (or big endian if p7)
	 *   optional [newline]
	 * @param filename
	 */
	public void initBinary(String filename) {
		try {
			int BUFFER_SIZE = 1024;
			File file = new File(filename);
			InputStream is = new FileInputStream(file);
			if (filename.endsWith(".gz")) {
				is = new GZIPInputStream(is, BUFFER_SIZE);
			} else {
				is = new BufferedInputStream(is, BUFFER_SIZE);
			}
			boolean isBE = file.getName().contains(".BEbin");
			DataInputStream be_in =             isBE ? new DataInputStream(is) : null;
			LittleEndianDataInputStream le_in = isBE ?                    null : new LittleEndianDataInputStream(is);
			String encoding = defaultEncoding;
			InputStream in = le_in != null ? le_in : be_in;
			
			this.numItems = Integer.parseInt(readText(in, ' ', encoding));
			this.dims = Integer.parseInt(readText(in, '\n', encoding));
			
			System.out.println("Loading "+numItems+" with "+dims+" dimensions");
			
			this.iter = new ThreadedLoopIterator<Pair<String,float[]>>() {
				@Override
				protected void loop() {
					try {
					for (int wordi = 0; wordi < numItems; ++wordi) {
						String word = readText(in, ' ', encoding).replace('_', ' ');
						
						float[] vector = new float[dims];
						for (int dimi = 0; dimi < dims; ++dimi) {
							if (le_in != null)
								vector[dimi] = le_in.readFloat();
							else
								vector[dimi] = be_in.readFloat();
						}
						add(Pair.of(word, vector));
						
						if ((wordi) % 100000 == 0)
							System.out.println("loading word "+wordi+": "+word);
					}
					
					in.close();
					} catch (Exception e) {
						Lang.error(e);
					}
				}
			};
			
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	
	private static final boolean validate = false;
	
	/**
	    Example Args:
	    wordEmbeddings/googles/vectors_en.BEbin
	    wordEmbeddings/ef/vectors_en.ef
	    wordEmbeddings/googles/vectors_en.properties
	 * @param args
	 */
	public static void main(String[] args) {
		/* CONSIDER:
		Options options = new Options();
		options.addOption("data", true, "directory containing the tensor input files");
		options.addOption("hypers", true, "properties file of hyper parameters");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);  
		} catch (ParseException pe) {
			Lang.error(pe);
		}
		*/
		Word2VecConverter w2v = new Word2VecConverter();
		//CONSIDER: check if extension is 'txt', 'bin' or 'BEbin' FileUtil.getExtension(args[0])
		if (args[0].endsWith("txt"))
			w2v.initText(args[0]);
		else
			w2v.initBinary(args[0]);
		EmbeddingFormat ef = new EmbeddingFormat();
		ef.shapes = new int[1][1];
		ef.itemSize = w2v.dims;
		ef.shapes[0][0] = w2v.dims;
		
		String saveTo = args.length > 1 ? args[1] : args[0]+".ef";
		
		if (args.length > 2) {
			ef.hyperparameters = PropertyLoader.fromString(FileUtil.readFileAsString(args[2]));
		} else {
			ef.hyperparameters = new Properties();
		}
		ef.name = ef.hyperparameters.getProperty("name", "word2vec");
		ef.write(new File(saveTo), w2v.numItems, w2v.iter);
		
		//CONSIDER: cmd line option
		if (validate) {
			EmbeddingFormat ef2 = new EmbeddingFormat();
			ef2.read(new File(saveTo));
		}
	}
}
