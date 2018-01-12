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

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.ibm.research.ai.ki.util.*;

/**
 * String values are stored UTF-8. Floats and ints are in network byte order = java standard = big endian.
 * @author mrglass
 *
 */
public class EmbeddingFormat {	
	/**
	 * The name of the model that created the embeddings (ex. word2vec)
	 */
	public String name;
	/**
	 * The hyperparameters of the model (ex. cbow=1; windowSize = 8; numNegative = 25;)
	 */
	public Properties hyperparameters;
	/**
	 * The string ids for the items
	 */
	public String[] items;
	/**
	 * The embeddings for the items, reshaped as a single vector
	 */
	public float[][] vectors;
	/**
	 * The shape of the tensors that are the embeddings for the items.
	 * If this is a vector-store then shapes.length == 1, shapes[0].length == 1, shapes[0][0] == itemSize
	 *   (one tensor, one dimensional).
	 */
	public int[][] shapes; 
	/**
	 * The total size of the embedding for each item (the number of floating point values).
	 * This either matches the shapes, or is zero for metadata and vocabulary only.
	 */
	public int itemSize;

	/**
	 * print progress while reading file
	 */
	public boolean verbose = false;
	
	/**
	 * Compute the cosine similarity of the two vectors
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static double cosine(float[] v1, float[] v2) {
		if (v1.length != v2.length)
			throw new IllegalArgumentException("vectors not same dimension: "+v1.length+" != "+v2.length);
		double numer = 0;
		double len1 = 0;
		double len2 = 0;
		for (int i = 0; i < v1.length; ++i) {
			double d1 = v1[i];
			double d2 = v2[i];
			numer += d1 * d2;
			len2 += d2 * d2;
			len1 += d1 * d1;
		}
		double length = Math.sqrt(len1 * len2); 
		if (length == 0) {
			return 0;
		}
		
		return numer / length;
	}
	
	public EmbeddingFormat() {}
	public EmbeddingFormat(File file) {
		read(file);
	}
	public EmbeddingFormat(File file, boolean vocabOnly) {
		read(file, vocabOnly ? new VocabOnlyLoad() : new FullyLoad());
	}
	
	public Map<String,Integer> getItem2Ndx() {
		Map<String,Integer> id2ndx = new HashMap<>();
		for (int i = 0; i <  items.length; ++i)
			id2ndx.put(items[i], i);
		return id2ndx;
	}
	public Map<String,Integer> getItem2Ndx(Function<String,String> normalizer) {
		Map<String,Integer> id2ndx = new HashMap<>();
		for (int i = 0; i <  items.length; ++i) {
			String normalized = normalizer.apply(items[i]);
			Integer first = id2ndx.put(normalized, i);
			if (first != null) {
				id2ndx.put(normalized, first);
			}
		}
		return id2ndx;
	}
	
	private static int[][] readShapes(DataInput in) {
		try {
			int numTensors = in.readInt();
			int[][] shapes = new int[numTensors][];
			for (int ti = 0; ti < numTensors; ++ti) {
				int dims = in.readInt();
				int[] shape = new int[dims];
				for (int si = 0; si < shape.length; ++si)
					shape[si] = in.readInt();
				shapes[ti] = shape;
			}
			return shapes;
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	private static void writeShapes(DataOutputStream out, int[][] shapes) {
		try {
			out.writeInt(shapes.length);
			for (int ti = 0; ti < shapes.length; ++ti) {
				out.writeInt(shapes[ti].length);
				for (int s : shapes[ti])
					out.writeInt(s);
			}
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	

	/**
	 * The DataInput.readUTF and writeUTF are a bit hard to write in anything other than java.
	 * It's best to make a more clear implementation: number of bytes as int, then true UTF-8
	 * and a null terminator (0 as a byte).
	 * @param in
	 * @return
	 */
	public static String readUTFSimpler(DataInput in) {
		try {
			int numBytes = in.readInt();
			byte[] raw = new byte[numBytes];
			in.readFully(raw);
			String str = new String(raw, "UTF-8");
			if (0 != in.readByte())
				throw new Error("Bad format! "+str);
			return str;
		} catch (Exception e) {
			throw new Error(e);
		}
	}	
	
	/**
	 * The DataInput.readUTF and writeUTF are a bit hard to write in anything other than java.
	 * It's best to make a more clear implementation: number of bytes as int, then true UTF-8
	 * and a null terminator (0 as a byte).
	 * @param out
	 * @param str
	 */
	public static void writeUTFSimpler(DataOutput out, String str) {
		try {
			byte[] raw = str.getBytes("UTF-8");
			out.writeInt(raw.length);
			out.write(raw);
			out.writeByte(0);
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	/**
	 * Get the item size based on the shape metadata. The field itemSize might be zero if this is a vocabulary only embedding file.
	 * @return
	 */
	public int calculateItemSize() {
		int itemSizeCalc = 0;
		for (int[] shape : shapes) {
			int size = 1;
			for (int si : shape)
				size *= si;
			itemSizeCalc += size;
		}
		return itemSizeCalc;
	}
	
	public interface VectorLoader {
		public void init(int numItems, int itemSize);
		public void load(int ndx, String item, float[] vector);
	}
	
	public class FullyLoad implements VectorLoader {
		@Override
		public void init(int numItems, int itemSize) {
			items = new String[numItems];
			vectors = new float[numItems][];
		}
		@Override
		public void load(int ndx, String item, float[] vector) {
			items[ndx] = item;
			vectors[ndx] = vector;
		}	
	}
	public class VocabOnlyLoad implements VectorLoader {
		@Override
		public void init(int numItems, int itemSize) {
			items = new String[numItems];
		}
		@Override
		public void load(int ndx, String item, float[] vector) {
			items[ndx] = item;
		}	
	}
	
	public static class Reader {
		public EmbeddingFormat metadata;
		public int numItems;
		public Iterator<Pair<String,float[]>> iter;
		
		//TODO: make the read method use this?
		public Reader(File file) {
			try {
				DataInputStream in = getDataInput(file);
				
				//CONSIDER: read magic number?
				metadata = new EmbeddingFormat();
				//read name
				metadata.name = readUTFSimpler(in);
				
				//read hyperparameters
				metadata.hyperparameters = new Properties();
				metadata.hyperparameters.load(new StringReader(readUTFSimpler(in)));

				//read number of items and shapes of each item	
				metadata.shapes = readShapes(in);
				
				metadata.itemSize = in.readInt();
				if (metadata.itemSize != 0 && metadata.itemSize != metadata.calculateItemSize())
					throw new Error("item size does not match shapes! "+metadata.itemSize+" vs. "+metadata.calculateItemSize());
				if (metadata.itemSize == 0 && metadata.verbose)
					System.out.println("Vocabulary and metadata only");
				
				numItems = in.readInt();
				
				iter = new Iterator<Pair<String,float[]>>() {
					int wordi = 0;

					@Override
					public boolean hasNext() {
						return wordi < numItems;
					}

					@Override
					public Pair<String, float[]> next() {
						String item = readUTFSimpler(in);
						float[] vector = null;
						if (metadata.itemSize != 0) {
							vector = new float[metadata.itemSize];
							for (int dimi = 0; dimi < metadata.itemSize; ++dimi) {
								try {
								vector[dimi] = in.readFloat();
								} catch (Exception e) {
									System.err.println("On "+wordi+" dim "+dimi);
									Lang.error(e);									}
							}
						}
						++wordi;
						if (wordi == numItems) {
							try {
								in.close();
							} catch (Exception e){}
						}
						return Pair.of(item, vector);
					}
				};
		
			} catch (IOException e) {
				Lang.error(e);
			}
		}
	}
	
	//TODO: support reading from DataInputStream so we can read ef from hdfs
	private static DataInputStream getDataInput(File file) {
		if (file == null) {
			throw new IllegalArgumentException("Null file!");
		}
		if (!file.exists()) {
			throw new IllegalArgumentException("No such file: "+file.getAbsolutePath());
		}
		//Or FileUtil.getDataInput(filename)
		try {
			int BUFFER_SIZE = 1024;
			InputStream is = new FileInputStream(file);
			if (file.getName().endsWith(".gz")) {
				is = new GZIPInputStream(is, BUFFER_SIZE);
			} else {
				is = new BufferedInputStream(is, BUFFER_SIZE);
			}
			return new DataInputStream(is);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	/**
	 * Strings written as: [number of bytes as int32][utf-8 encoded string][0 byte]
	 * All numbers are network byte order.
	 * 
	 * Format:
	 * modelName (as string)
	 * hyperparameters (as string)
	 * shapes of items 
	 *   (int32 for number of tensors)
	 *   for each tensor 
	 *     (int32 number of dimensions)
	 *     for each dimension
	 *       (int32 for size of this dimension)
	 * itemSize (as int32; either zero to indicate only the vocabulary is stored (for Annoy) or the actual itemSize which should match the shape) 
	 * number of items (as int32)
	 * for each item
	 *   item name (as string)
	 *   values of embedding (as float32 stored in same order as multidimensional array in C)
	 * 
	 * The zero byte terminating the strings is just for validation, to ensure there isn't some off-by-one error resulting in the 
	 * embeddings being loaded as garbage.
	 * 
	 * @param file
	 */
	public void read(File file) {
		read(file, null);
	}
	public void read(File file, VectorLoader loader) {
		try (DataInputStream in = getDataInput(file)) {
			read((DataInput)in, loader);
		} catch (IOException e) {
			Lang.error(e);
		}
	}

	/**
	 * Note does not close the InputStream
	 */
	public void read( InputStream stream, VectorLoader loader ) {
		final DataInput in = new DataInputStream(stream);
		read(in, loader);
	}

	/**
	 * Note does not close the DataInput (doesn't even know if it is a stream)
	 * @param in
	 * @param loader
	 */
	public void read(DataInput in, VectorLoader loader) {
		try {
			//CONSIDER: read magic number?

			//read name
			name = readUTFSimpler(in);
			
			//read hyperparameters
			hyperparameters = new Properties();
			hyperparameters.load(new StringReader(readUTFSimpler(in)));

			//read number of items and shapes of each item	
			this.shapes = readShapes(in);
			
			itemSize = in.readInt();
			if (itemSize != 0 && itemSize != calculateItemSize())
				throw new Error("item size does not match shapes! "+itemSize+" vs. "+calculateItemSize());
			if (itemSize == 0 && verbose)
				System.out.println("Vocabulary and metadata only");
			
			if (loader == null)
				loader = itemSize == 0 ? new VocabOnlyLoad() : new FullyLoad();
			
			int numItems = in.readInt();
			
			loader.init(numItems, itemSize);
			
			if (verbose) {
				System.out.println("Loading "+numItems+" with "+itemSize+" size"); System.out.flush();
			}

			for (int wordi = 0; wordi < numItems; ++wordi) {
				String item = readUTFSimpler(in);
				float[] vector = null;
				if (itemSize != 0) {
					vector = new float[itemSize];
					for (int dimi = 0; dimi < itemSize; ++dimi) {
						try {
						vector[dimi] = in.readFloat();
						} catch (EOFException e) {
							System.err.println("On "+wordi+" dim "+dimi);
							throw e;
						}
					}
				}
				loader.load(wordi, item, vector);
				
				if (verbose && wordi % 100000 == 0)
					System.out.println("loading word "+wordi+": "+item);
			}
		} catch (IOException e) {
			Lang.error(e);
		}		
	}
	
	class ItemVectorIter implements Iterator<Pair<String,float[]>> {
		ItemVectorIter() {
			wordi = 0;
			if (vectors != null && items.length != vectors.length) {
				throw new IllegalStateException(items.length + " != " + vectors.length);
			}
		}
		int wordi = 0;
		@Override
		public boolean hasNext() {
			return wordi < items.length;
		}

		@Override
		public Pair<String, float[]> next() {
			Pair<String,float[]> ret = Pair.of(items[wordi], vectors == null ? null : vectors[wordi]);
			++wordi;
			return ret;
		}
		
	}
	public void write(File file) {
		write(file, this.items.length, new ItemVectorIter());
	}
	
	public void write(File file, int numItems, Iterator<Pair<String,float[]>> iter) {
		try {
			int BUFFER_SIZE = 1024;
			FileUtil.ensureWriteable(file);
			OutputStream os = new FileOutputStream(file);
			if (file.getName().endsWith(".gz")) {
				os = new GZIPOutputStream(os, BUFFER_SIZE);
			} else {
				os = new BufferedOutputStream(os, BUFFER_SIZE);
			}
			write( os, numItems, iter);
			os.close();
		} catch (Exception e) {
			Lang.error(e);
		}
		
	}

	/**
	 * Write to provided stream, The Stream is NOT closed
	 */
	public void write(OutputStream stream, int numItems, Iterator<Pair<String,float[]>> iter) {
		try {
			DataOutputStream out = new DataOutputStream(stream);

			writeUTFSimpler(out, name);

			StringWriter writer = new StringWriter();
			hyperparameters.store(writer, null);
			writeUTFSimpler(out, writer.toString());

			writeShapes(out, shapes);

			out.writeInt(itemSize);
			out.writeInt(numItems);

			int itemCount = 0;
			while (iter.hasNext()) {
				++itemCount;
				Pair<String,float[]> wv = iter.next();
				writeUTFSimpler(out, wv.first);
				if (wv.second != null)
					for (float f : wv.second) {
						out.writeFloat(f);
					}
			}
			if (itemCount != numItems)
				throw new IllegalArgumentException("expecting "+numItems+" but read "+itemCount);
			out.flush();
		} catch (Exception e) {
			Lang.error(e);
		}

	}

}
