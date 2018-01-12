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
package com.ibm.research.ai.ki.util;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;


public class Lang {
  
  /**
   * pads the str on the right (RPAD) with enough characters of ' ' to make it have length len.
   * if the string is longer than len, it is truncated. the returned string will always have length = len
   * @param str : input String to be right padded
   * @param c : Char to be padded at the right
   * @param len : Total length of the return String
   * @return : padded String
   */
  public static String RPAD(String str, int len) {
    if (str == null) {
      return "";
    }
    int padLen = len - str.length();
    if (padLen < 0) {
      return str.substring(0, len);
    }
    StringBuffer sb = new StringBuffer(str);
    for (int i = 0; i < padLen; i++) {
      sb.append(' ');
    }
    return sb.toString();
  }
  
  
  /**
   * pads the str on the right (RPAD) with enough characters 'c' to make it have length len.
   * if the string is longer than len, it is truncated. the returned string will always have length = len
   * @param str : input String to be right padded
   * @param c : Char to be padded at the right
   * @param len : Total length of the return String
   * @return : padded String
   */
  public static String RPAD(String str, char c, int len) {
    if (str == null) {
      return null;
    }
    int padLen = len - str.length();
    if (padLen < 0) {
      return str.substring(0, len);
    }
    StringBuffer sb = new StringBuffer(str);
    for (int i = 0; i < padLen; i++) {
      sb.append(c);
    }
    return sb.toString();
  }


  /**
   * pads the str on the left (LPAD) with enough characters of ' ' to make it have length len.
   * if the string is longer than len, it is truncated. the returned string will always have length = len
   * @param str
   * @param c
   * @param len
   * @return
   */
  public static String LPAD(String str, int len) {
    return LPAD(str, ' ', len);
  }
  

  /**
   * pads the str on the left (LPAD) with enough characters 'c' to make it have length len.
   * if the string is longer than len, it is truncated. the returned string will always have length = len
   * @param str
   * @param c
   * @param len
   * @return
   */
  public static String LPAD(String str, char c, int len) {
    if (str == null) {
      return null;
    }
    int padLen = len - str.length();
    if (padLen < 0) {
      return str.substring(0, len);
    }
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < padLen; i++) {
      sb.append(c);
    }
    sb.append(str);
    return sb.toString();
  }


  /**
   * if the string is longer than len, only its len first c characters are returned, otherwise the string itself is returned
   * @param str : Input String to be truncated
   * @param len : Total length of the return String.
   * @return    : truncated String
   */
  public static String truncate(String str, int len) {
    if (str ==null) {
      return str;
    }
    int tLen = len - str.length();
    if (tLen < 0) {
      return str.substring(0, len);
    }
    return str;
  }

  /**
   * if the first argument is non-null it returns it, otherwise it returns the second argument
   * @param first : Input object 1
   * @param second : Input object 2
   * @return : Object
   */
  public static <T> T NVL(T first, T second) {
    return first == null ? second : first;
  }

  /**
   * Returns the first index of the Obj in the array if it is present, otherwise returns -1
   * @param oArray
   * @param obj
   * @return int
   */
  public static int linearSearch(Object[] oArray, Object obj) {
    if (oArray == null || obj == null) {
      return -1;
    }
    int oLen = oArray.length;
    for (int i = 0; i < oLen; i++) {
      if (oArray[i].equals(obj)) {
        return i;
      }
    }
    return -1;
  }
  public static int linearSearch(Iterable<?> oArray, Object obj) {
	    if (oArray == null || obj == null) {
	      return -1;
	    }
	    int ndx = 0;
	    for (Object o : oArray) {
	      if (o.equals(obj)) {
	        return ndx;
	      }
	      ++ndx;
	    }
	    return -1;
	  }
  
  /**
   * Returns true iff the string is an integer
   * @param String
   * @return boolean
   */
  public static boolean isInteger(String s) {
    try {
      Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if the string is an double
   * @param s
   * @return boolean
   */
  public static boolean isDouble(String s) { //
    try {
      Double.parseDouble(s);
      return true;
    } catch (Exception e) {
    }
    return false;
  }

  /**
   * returns the toString of the objects (in order) separated by 'separator'
   * @param array
   * @param separator
   * @return
   */
  public static String stringList(Object[] array, String separator) {
    if (array == null)
      return null;
    StringBuilder sb = new StringBuilder();
    int oLen = array.length;
    for (int i = 0; i < oLen; i++) {
      sb.append(array[i] == null ? array[i] : array[i].toString());
      if (i != (oLen - 1)) {
        sb.append(separator);
      }
    }
    return sb.toString();
  }


  /**
   * returns the toString of the objects (in order) separated by 'separator'
   * @param Iterable array
   * @param separator
   * @return
   */
	public static String stringList(Iterable<?> array, String separator) {
		if (array == null) {
			return null;
		}
		StringBuffer sb = new StringBuffer();
		Iterator<?> it = array.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			sb.append(o != null ? o.toString() : o);
			if (it.hasNext()) {
				sb.append(separator);
			}
		}
		return sb.toString();
	}

	private static DecimalFormat shortFormat = new DecimalFormat("0.000");
	public static String dblStr(double x) {
		return shortFormat.format(x);
	}
	
	public static String milliStr(long milliseconds) {
		if (milliseconds < 1000) {
			return milliseconds+" milliseconds";
		}
		if (milliseconds < 60 * 1000) {
			return dblStr(milliseconds/1000.0)+" seconds";
		}
		if (milliseconds < 60 * 60 * 1000) {
			return dblStr(milliseconds/(60*1000.0))+" minutes";
		}
		if (milliseconds < 24 * 60 * 60 * 1000) {
			return dblStr(milliseconds/(60*60*1000.0))+" hours";
		}
		return dblStr(milliseconds/(24*60*60*1000.0))+" days";
	}  

	/**
	 * Sets the list[ndx] = item, filling with nulls if needed to make the size >= ndx+1
	 * @param list
	 * @param ndx
	 * @param item
	 * @return
	 */
	public static <T> T setAtFill(List<T> list, int ndx, T item) {
		while (list.size() <= ndx)
			list.add(null);
		return list.set(ndx, item);
	}
	
	/**
	 * get the classpath that was passed to the JVM
	 * @return
	 */
	public static String getClasspath() {
		ClassLoader cl = ClassLoader.getSystemClassLoader();
	    URL[] urls = ((URLClassLoader)cl).getURLs();
	    StringBuffer classpath = new StringBuffer();
	    for(URL url: urls){
	    	String f = url.getFile();
	    	
	    	if (classpath.length() != 0) {
	    		classpath.append(File.pathSeparator);
	    	}
	    	classpath.append(f);
	    	/*
	    	int wksp = f.indexOf("workspace"+File.separatorChar);
	    	String add = f.substring(wksp+"workspace".length());
	    	classpath.append("$workspace").append(add);
	    	*/
	    	//System.out.println(url.getFile());
	    }
	    return classpath.toString();
	}
	
	/**
	 * programmatically add to the classpath
	 */
	public static void addPath(URLClassLoader urlClassLoader, URL u) {
		try {
		    Class<URLClassLoader> urlClass = URLClassLoader.class;
		    Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
		    method.setAccessible(true);
		    method.invoke(urlClassLoader, new Object[]{u});
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	/**
	 * run your program with the JVM flag: -verbose:class
	 * save the output in a file and pass the filename to this function
	 * you will get all the locations and jar files where classes are loaded from
	 * @param verboseclassOut
	 */
	public static void loadedDependencies(String verboseclassOut) {
		Set<String> deps = new HashSet<>();
		Pattern loadedClassFrom = Pattern.compile(
				"(?:"+Pattern.quote("[1] ")+")?" //added by java-viaducc
				+ "(?:\\[Loaded |class load:)" //sun vs. ibm java
				+ "(.*?)"
				+ " from(?::)? " //ibm java adds ':'
				+ "(.*?)"
				+ "\\]");
		for (String line : FileUtil.getLines(verboseclassOut)) {
			if (!line.endsWith("]"))
				line = line + "]";
			Matcher m = loadedClassFrom.matcher(line);
			if (!m.find() || m.end() != line.length())
				continue;
			String classname = m.group(1);
			String from = m.group(2);
			deps.add(from);
		}
		List<String> depList = new ArrayList<>(deps);
		Collections.sort(depList);
		System.out.println(Lang.stringList(depList, "\n"));
	}
	
	public static String stackString(Throwable t) {
		Writer w = new StringWriter();
		PrintWriter pw = new PrintWriter(w);
		t.printStackTrace(pw);
		return w.toString();
	}
	
	/**
	 * The alphanumeric characters "a" through "z", "A" through "Z" and "0" through "9" remain the same.
	 * The special characters ".", "-", "*", and "_" remain the same.
	 * The space character " " is converted into a plus sign "+".
	 * All other characters converted into '%XY'
	 * @param preEncode
	 * @return
	 */
	public static String urlEncode(String preEncode) {
		if (preEncode == null) {
			throw new IllegalArgumentException("Null in Lang.urlEcode");
		}
		try {
			return URLEncoder.encode(preEncode, "UTF-8");
		} catch (Exception e) {throw new Error(e);}
	}
	public static String fullURLEncode(String preEncode) {
		return urlEncode(preEncode).replace(".", "%2E").replace("-", "%2D").replace("*", "%2A").replace("_", "%5F").replace("+", "%20");
	}
	public static String urlDecode(String encoded) {
		try {
			return URLDecoder.decode(encoded, "UTF-8");
		} catch (Exception e) {
			System.err.println("for encoded = "+encoded);
			return Lang.error(e);
		}
	}	
	
	public static <T> T error(Throwable t) {
		if (t instanceof RuntimeException)
			throw (RuntimeException)t;
		else if (t instanceof Error)
			throw (Error)t;
		
		throw new RuntimeException(t);
	}
	
	/**
	 * copies the entire object graph reachable from obj
	 * all the objects must be serializable for this to work
	 * @param obj
	 * @return
	 */
	public static <T extends Serializable> T deepCopy(T obj) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			return (T) ois.readObject();
		} catch (IOException ioe) {
			return error(ioe);
		} catch (ClassNotFoundException cfe) {
			return error(cfe);
		}
	}
	
	public static final String pWhite_Space = "[\\u0009"
                        + "\\u000A" // LINE FEED (LF)
                        + "\\u000B" // LINE TABULATION
                        + "\\u000C" // FORM FEED (FF)
                        + "\\u000D" // CARRIAGE RETURN (CR)
                        + "\\u0020" // SPACE
                        + "\\u0085" // NEXT LINE (NEL) 
                        + "\\u00A0" // NO-BREAK SPACE
                        + "\\u1680" // OGHAM SPACE MARK
                        + "\\u180E" // MONGOLIAN VOWEL SEPARATOR
                        + "\\u2000" // EN QUAD 
                        + "\\u2001" // EM QUAD 
                        + "\\u2002" // EN SPACE
                        + "\\u2003" // EM SPACE
                        + "\\u2004" // THREE-PER-EM SPACE
                        + "\\u2005" // FOUR-PER-EM SPACE
                        + "\\u2006" // SIX-PER-EM SPACE
                        + "\\u2007" // FIGURE SPACE
                        + "\\u2008" // PUNCTUATION SPACE
                        + "\\u2009" // THIN SPACE
                        + "\\u200A" // HAIR SPACE
                        + "\\u2028" // LINE SEPARATOR
                        + "\\u2029" // PARAGRAPH SEPARATOR
                        + "\\u202F" // NARROW NO-BREAK SPACE
                        + "\\u205F" // MEDIUM MATHEMATICAL SPACE
                        + "\\u3000]"; // IDEOGRAPHIC SPACE";
}
