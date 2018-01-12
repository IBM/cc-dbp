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
package com.ibm.research.ai.ki.util.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;
/**
 * Reads the tensors from the files in the directory. Files are random order and items within files are block-shuffled.
 * @author mrglass
 *
 */
public class TensorFileReader implements Iterable<Object[]> {

    //FIXME: remove this to use a more consistent approach to string tensors
    static final boolean oldStyleString = false;
    
	public TensorFileReader(File dir) {
		this(dir, 1.0);
	}
	public TensorFileReader(File dir, double datasetFilesPercent) {
		this.rootDir = dir;
		this.datasetPercent = datasetFilesPercent;
		files = getFraction(new FileUtil.FileIterable(rootDir, null, new Random()));
	}
	
	final double datasetPercent;
	
	final File rootDir;
	int shuffleBlockSize = 1000; //FIXME: less memory intensive, maybe using a random access file with a tensorset index
	boolean reportedDownsampling = false;
	List<File> files;
	
	protected List<File> getFraction(Iterable<File> files) {
		if (datasetPercent >= 1.0 || datasetPercent <= 0.0) {
		    List<File> fs = new ArrayList<>();
		    for (File f : files)
		        fs.add(f);
			return fs;
		}
		List<Pair<File,Double>> allFiles = new ArrayList<>();
		for (File f : files)
			allFiles.add(Pair.of(f, RandomUtil.pseudoRandomFromString(f.getAbsolutePath())));
		SecondPairComparator.sort(allFiles);
		List<File> ffiles = new ArrayList<>();
		for (Pair<File,Double> f : allFiles) {
			if (ffiles.size() >= allFiles.size() * datasetPercent)
				break;
			ffiles.add(f.first);
		}
		Collections.shuffle(ffiles);
		if (!reportedDownsampling) {
		    System.out.println("TensorFileReader: downsampling from "+allFiles.size()+" files to "+ffiles.size()+" files");
		    reportedDownsampling = true;
		}
		return ffiles;
	}
	
	@Override
	public Iterator<Object[]> iterator() {
	    
		return new ThreadedLoopIterator<Object[]>(shuffleBlockSize+1) {
			@Override
			protected void loop() {
			    Collections.shuffle(files);
				List<Object[]> shuffleBlock = new ArrayList<Object[]>(shuffleBlockSize);
				for (File f : files) {
					try (DataInputStream in = FileUtil.getDataInput(f.getAbsolutePath())) {
						while (true) {
							Object[] objs = readTensorSet(in);
							shuffleBlock.add(objs);
							if (shuffleBlock.size() >= shuffleBlockSize) {
								Collections.shuffle(shuffleBlock);
								for (Object[] oi : shuffleBlock)
									super.add(oi);
								shuffleBlock.clear();
							}
						}
					} catch (EOFException eof) {
						//done
					} catch (Exception e) {
						Lang.error(e);
					}
				}
				Collections.shuffle(shuffleBlock);
				for (Object[] oi : shuffleBlock)
					super.add(oi);
				shuffleBlock.clear();
			}		
		};
	}
	
	public static Object[] readTensorSet(byte[] bytes) {
		try {
			return readTensorSet(new DataInputStream(new ByteArrayInputStream(bytes)));
		} catch (IOException e) {
			throw new Error(e);
		}
	}
	
	public static Object[] readTensorSet(DataInput in) throws IOException {
	    
		//read number of tensors from file
		int numTensors = in.readInt();
		Object[] objs = new Object[numTensors]; 
		for (int ti = 0; ti < objs.length; ++ti) {
			//read type from file
			Class type = TensorFileWriter.fromTypeId(in.readByte());
			if (oldStyleString && type == String.class) {
				//just permit String as a possible type
				//will need a DummyInputTensor for ModelDescription construction
				objs[ti] = DataIO.readUTFSimpler(in);
			} else {
				//read shape and values
				objs[ti] = readNDArray(in, type); 
			}
			if (objs[ti] == null) {
				if (ti != 0) {
					throw new Error("file ended in the middle of a tensor set!");
				}
				throw new EOFException();
			}
		}
		if (0 != in.readByte()) {
			throw new Error("canary value not present - file is corrupted or not of the expected tensor types.");
		}
		return objs;
	}
	
	static Object readArray(DataInput in, Class expectedType, int len) {
		try {
			if (expectedType == int.class) { 
				int[] vi = new int[len];
				for (int i = 0; i < vi.length; ++i)
					vi[i] = in.readInt();
				return vi;
			}
			if (expectedType == float.class) {
				float[] vf = new float[len];
				for (int i = 0; i < vf.length; ++i)
					vf[i] = in.readFloat();
				return vf;
			}
			if (expectedType == double.class) {
				double[] vd = new double[len];
				for (int i = 0; i < vd.length; ++i)
					vd[i] = in.readDouble();
				return vd;
			}
			if (expectedType == String.class) {
			    String[] vs = new String[len];
			    for (int i = 0; i < vs.length; ++i)
                    vs[i] = DataIO.readUTFSimpler(in);
                return vs;
			}
			throw new IllegalArgumentException("Unsupported type: "+expectedType);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	static Object readNDArray(DataInput in, Class expectedType) {
		boolean pastStart = false;
		try {
			int numDims = in.readInt();
			pastStart = true;
			int[] shape = new int[numDims];
			for (int i = 0; i < shape.length; ++i)
				shape[i] = in.readInt();
			if (numDims == 0) {
				if (expectedType == int.class)
					return in.readInt();
				if (expectedType == float.class)
					return in.readFloat();
				if (expectedType == double.class)
					return in.readDouble();
				if (expectedType == String.class)
				    return DataIO.readUTFSimpler(in);
				throw new IllegalArgumentException("Unsupported type: "+expectedType);
			} else if (numDims == 1) {
				return readArray(in, expectedType, shape[0]);
			}
			
			Object[] outermost = 
					(Object[])Array.newInstance(expectedType, shape);
			int[] indices = new int[shape.length];
			int inc = shape[shape.length-1];
			do {
				setArray(outermost, indices, 0, readArray(in, expectedType, inc));
				indices[indices.length-1] = inc - 1;
			} while (TensorFileWriter.nextNdx(indices, shape));
			return outermost;
		} catch (EOFException eof) {
			if (pastStart)
				throw new Error("stream ended in the middle of an item!");
			return null;
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	public static void setArray(Object[] dbls, int[] indices, int start, Object arr) {
		if (start == indices.length-2) {
			dbls[indices[start]] = arr;
		} else {
			setArray((Object[])dbls[indices[start]], indices, start+1, arr);
		}	
	}
}
