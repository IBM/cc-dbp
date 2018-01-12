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

import com.ibm.research.ai.ki.util.*;

public class DataIO {
	/**
	 * The DataInput.readUTF and writeUTF are a bit hard to write in anything other than java.
	 * It's best to make a more clear implementation: number of bytes as int, then true UTF-8
	 * and a null terminator (0 as a byte).
	 * Returns null if EOF is already reached. If EOF is reached while reading it is an Error.
	 * @param in
	 * @return
	 */
	public static String readUTFSimpler(DataInput in) {
	    int numBytes;
	    try {
	        numBytes = in.readInt();
	    } catch (EOFException e) {
	        return null;
	    } catch (Exception e) {
	        return Lang.error(e);
	    }
		try {
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
			out.write(0);
		} catch (Exception e) {
			throw new Error(e);
		}
	}
}
