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
import java.util.*;

import org.apache.commons.lang3.*;

import com.ibm.research.ai.ki.util.*;

//tensors that have sizes given in file
public class TensorFileWriter implements AutoCloseable {
	private final int batchesPerFile;
	private int fileBatchCount = 0;
	private final File rootDir;
	private int fileCount = 0;
	
	protected DataOutputStream out;
	protected int[] sizes;
	
	public TensorFileWriter(File dir) {
		this(dir, 10000);
	}
	public TensorFileWriter(File dir, int batchesPerFile) {
        this.rootDir = dir;
        this.batchesPerFile = batchesPerFile;
    }
	
	protected DataOutputStream getOut() {
		++fileBatchCount;
		if (out == null || fileBatchCount > batchesPerFile) {
			try {
				if (out != null)
					out.close();
			} catch (Exception e) {
				Lang.error(e);
			}
			out = FileUtil.getDataOutput(new File(rootDir, fileCount+".bin.gz").getAbsolutePath());
			fileBatchCount = 0;
			++fileCount;
		}
		return out;
	}
	
	public static int toTypeId(Class cls) {
		if (cls == double.class)
			return 0;
		if (cls == float.class)
			return 1;
		if (cls == int.class)
			return 2;
		if (cls == String.class)
			return 3;
		throw new IllegalArgumentException("unsupported: "+cls);
	}
	
	public static Class fromTypeId(int typeId) {
		if (typeId == 0)
			return double.class;
		if (typeId == 1)
			return float.class;
		if (typeId == 2)
			return int.class;
		if (typeId == 3)
			return String.class;
		throw new IllegalArgumentException("unsupported: "+typeId);
	}
	
    public static void writeTensorSet(DataOutput out, Object... tensors) throws IOException {
        // write number of tensors
        out.writeInt(tensors.length);
        //loop over tensors, write num dims, shape, and the flat values
        for (Object t : tensors) {
            // write type
            out.writeByte(toTypeId(typeOf(t)));
            if (TensorFileReader.oldStyleString && t instanceof String) {
                DataIO.writeUTFSimpler(out, (String) t);
            } else {
                // write shape
                int[] shape = shapeOf(t);
                out.writeInt(shape.length);
                for (int si : shape)
                    out.writeInt(si);
                // write values
                writeFlat(t, shape, out);
            }
        }

        // then write the null byte as a canary value
        out.writeByte(0);
    }

	//version that outputs a byte[] (for Spark) - which may then be base64enc so we get one instance per line
	public static byte[] byteArrayTensorSet(Object... tensors) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			writeTensorSet(out, tensors);
			return baos.toByteArray();
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	public synchronized void addTensorSet(Object... tensors) {
		try {
			DataOutputStream out = getOut();
			writeTensorSet(out, tensors);
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	
	@Override
	public void close() {
		try {
			if (out != null) {
				out.close();
				out = null;
			}
		} catch (Exception e) {}
	}
	
	/**
	 * writes a multi-dimensional array, or scalar. Supported types are double, float and int.
	 * @param dbls
	 * @param shape the shape of the multi-dimensional array
	 */
	public static void writeFlat(Object dbls, int[] shape, DataOutput out) {
		try {
			if (dbls instanceof Double) {
				out.writeDouble((Double)dbls); return;
			} else if (dbls instanceof Float) {
				out.writeFloat((Float)dbls); return;
			} else if (dbls instanceof Integer) {
				out.writeInt((Integer)dbls); return;
			} else if (dbls instanceof String) {
			    DataIO.writeUTFSimpler(out, (String)dbls); return;
			}

			int[] indices = new int[shape.length];
			int inc = shape[shape.length-1];
			do {
				Object arr = indexToArray(dbls, indices, 0);
				if (arr instanceof double[]) {
					for (double v : (double[])arr)
						out.writeDouble(v);
				} else if (arr instanceof int[]) {
					for (int v : (int[])arr)
						out.writeInt(v);
				} else if (arr instanceof float[]) {
					for (float v : (float[])arr)
						out.writeFloat(v);
				} else if (arr instanceof String[]) {
				    for (String s : (String[])arr)
				        DataIO.writeUTFSimpler(out, s);
				} else {
					throw new IllegalArgumentException("Bad array type: "+arr.getClass().getCanonicalName());
				}
				indices[indices.length-1] = inc - 1;
			} while (nextNdx(indices, shape));
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	/**
	 * get the base level array from dbls specified by indices[start:indices.length-1]
	 * @param dbls a multidimensional array
	 * @param indices the indices into the dbls
	 * @param start the positioning that the indices starts at
	 * @return
	 */
	public static Object indexToArray(Object dbls, int[] indices, int start) {
		if (start == indices.length-1)
			return dbls;
		return indexToArray(((Object[])dbls)[indices[start]], indices, start+1);
	}
	
	/**
	 * get the index to the next element in an n-dimensional array with the given shape
	 * @param indices
	 * @param shape
	 * @return
	 */
	public static boolean nextNdx(int[] indices, int[] shape) {
		for (int i = indices.length-1; i >= 0; --i) {
			if (indices[i] < shape[i]-1) {
				++indices[i];
				return true;
			} else {
				indices[i] = 0;
			}
		}
		return false;
	}
	
	public static Class typeOf(Object ndarray) {
		if (ndarray instanceof Double)
			return double.class;
		if (ndarray instanceof Float)
			return float.class;
		if (ndarray instanceof Integer)
			return int.class;
		if (ndarray instanceof double[])
			return double.class;
		if (ndarray instanceof float[])
			return float.class;
		if (ndarray instanceof int[])
			return int.class;
		if (ndarray instanceof String)
		    return String.class;
		if (ndarray instanceof Object[])
			return typeOf(((Object[])ndarray)[0]);
		throw new IllegalArgumentException("Unsupported type: "+ndarray.getClass().getCanonicalName());
	}
	
	/**
	 * assumes the multi-dimensional array is well-shaped
	 * for example: all the dbls[i].length are equal; all the dbls[i][j].length are equal
	 * TODO: verify that the array is actually well-shaped as an SGDDebug option
	 * @param ndarray
	 */
	public static int[] shapeOf(Object ndarray) {
		if (ndarray instanceof Double || ndarray instanceof Float || ndarray instanceof Integer || ndarray instanceof String) {
			return new int[0];
		}
		
		Object curNDArray = ndarray;
		List<Integer> shape = new ArrayList<Integer>();
		while (true) {
		    if (curNDArray instanceof String[]) {
		        shape.add(((String[])curNDArray).length);
                break;
		    } else if (curNDArray instanceof Object[]) {
				shape.add(((Object[])curNDArray).length);
				curNDArray = ((Object[])curNDArray)[0];
			} else if (curNDArray instanceof double[]) {
				shape.add(((double[])curNDArray).length);
				break;
			} else if (curNDArray instanceof float[]) {
				shape.add(((float[])curNDArray).length);
				break;
			} else if (curNDArray instanceof int[]) {
				shape.add(((int[])curNDArray).length);
				break;
			} else {
				throw new IllegalArgumentException();
			}
		}
		return ArrayUtils.toPrimitive(shape.toArray(new Integer[shape.size()]));		
	}


}
