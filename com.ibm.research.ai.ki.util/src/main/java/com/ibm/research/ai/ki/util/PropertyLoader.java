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

import java.beans.*;
import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;

public class PropertyLoader {

  public static final String DFLT_ETN = ".properties";

    //NOTE: non-String keys or values are skipped
  	public static void setFieldsFromProperties(Object obj, Map<Object,Object> props) {
  		for (Map.Entry<Object, Object> e : props.entrySet()) {
  			if (e.getKey() instanceof String && e.getValue() instanceof String) {
  				String fieldName = (String)e.getKey();
  				String value = (String)e.getValue();
  				try {
  					Field f = obj.getClass().getField(fieldName);
  					f.setAccessible(true);
  					
  					PropertyEditor pe = PropertyEditorManager.findEditor(f.getType());
  					pe.setAsText(value);
  					f.set(obj, pe.getValue());	
  				} catch (NoSuchFieldException nsf) {
  					//fine, just skip
  				} catch (IllegalAccessException iae) {
  					Lang.error(iae);
  				}
  			}
  		}
  	}
  
  	/**
  	 * String representation of the properties
  	 * @param props
  	 * @return
  	 */
	public static String toString(Properties props) {
		StringWriter writer = new StringWriter();
		try {
			props.store(writer, "");
		} catch (IOException e) {
			Lang.error(e);
		}
		return writer.getBuffer().toString();
	}
  	
	/**
	 * creates Properties map from the string
	 * @param propStr
	 * @return
	 */
	public static Properties fromString(String propStr) {
	    if (propStr == null)
	        throw new IllegalArgumentException("Property string cannot be null!");
		Properties props = new Properties();
		try {
			props.load(new StringReader(propStr));
		} catch (IOException e) {
			Lang.error(e);
		}
		return props;
	}
	
	/**
   * Checks classpath for a properties file 'name'
   * Works with either '.' separator or '/' separator and with or without '.properties' extension
   * if the .properties file contains things like keyX=bar, keyY=${keyX}foo 
   * then the ${keyX} will be substituted with the keyX value
   * 
	 * @param name qualified name of properties file
	 * @return
	 */
	public static Properties getPropertiesWithSubstitutions(String name) {
		return propertiesWithSubstitutions(loadProperties(name));
	}

    /**
     * If the Properties contains things like keyX=bar, keyY=${keyX}foo then the
     * ${keyX} will be substituted with the keyX value
     * 
     * @param name qualified name of properties file
     * @return
     */
    public static Properties propertiesWithSubstitutions(Properties props) {
        HashMap<String, String> currentMap = new HashMap<String, String>();
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            if (e.getKey() instanceof String && e.getValue() instanceof String) {
                currentMap.put((String) e.getKey(), (String) e.getValue());
            } else {
                throw new IllegalArgumentException("Bad property: " + e);
            }
        }
        // do replacement on the properties
        HashMap<String, String> nextMap = null;
        boolean modified = true;
        while (modified) {
            modified = false;
            nextMap = new HashMap<String, String>();
            for (Map.Entry<String, String> e : currentMap.entrySet()) {
                String path = e.getValue();
                for (Map.Entry<String, String> oe : currentMap.entrySet()) {
                    if (e.getKey() != oe.getKey()) {
                        String nextpath = path.replace("${" + oe.getKey() + "}", oe.getValue());
                        if (!path.equals(nextpath)) {
                            path = nextpath;
                            modified = true;
                        }
                    }
                }
                nextMap.put(e.getKey(), path);
            }
            currentMap = nextMap;
        }
        Properties propsSubst = new Properties();
        propsSubst.putAll(currentMap);
        return propsSubst;
    }
	
  /**
   * loads the properties file 'name' from the classpath - same classloader that loaded PropertyLoader. it is very
   * permissive in how the name is formated - it can have the '.properties' extension or not it can
   * use either '.' or '/' as a separator there can be a leading '/' or not Example: Properties mine
   * = PropertyLoader.loadProperties("com.ibm.bluej.mypackage.myprops") loads 'myprops.properties'
   * from a directory like com/ibm/bluej/mypackage in the classpath
   * 
   * @param name
   * @return
   */
  public static Properties loadProperties(String name) {
    //System.out.println(getName(name));
    return getProperties(getClassLoader(), getName(name));
  }

  private static ClassLoader getClassLoader() {
	  return PropertyLoader.class.getClassLoader();
    // return  ClassLoader.getSystemClassLoader();
  }

  /**
   * returns URL for 'name' from the classpath - same classloader that loaded PropertyLoader. it is very
   * permissive in how the name is formated - it can have the '.properties' extension or not it can
   * use either '.' or '/' as a separator there can be a leading '/' or not.
   * 
   * @param name
   * @return
   */
  public static URL getUrl(String name) {
	  return getClassLoader().getResource(getName(name));
  }
  
  private static Properties getProperties(ClassLoader loader, String resource) {
    Properties props = new Properties();
    InputStream stream = loader.getResourceAsStream(resource);
    if (stream == null) {
    	try {
			stream = new BufferedInputStream(new FileInputStream('/'+resource));
		} catch (FileNotFoundException e) {
			stream = null;
		}
    }
    if (stream == null) {
        try {
            stream = new BufferedInputStream(new FileInputStream(resource));
        } catch (FileNotFoundException e) {
            stream = null;
        }
    }
    if (stream == null) {
      throw new IllegalArgumentException("Specified resource not found " + resource);
    } else {
      try {
        props.load(stream);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load resource" + resource);
      } finally {
        if (stream != null) {
          try {
            stream.close();
          } catch (IOException e) {
          }
        }
      }
    }
    return props;
  }

  private static String getName(String name) {
    // there can be a leading '/' or not. Remove it
    if (name.startsWith(File.separator)) {
      name = name.substring(1);
    }
    // use either '.' or '/' as a separator
    if (name.endsWith(DFLT_ETN)){
      name = name.substring(0,name.length()-DFLT_ETN.length());
    }
    name = name.replace('.', File.separatorChar);
    
    if (!name.endsWith(DFLT_ETN)) {
      name = name + DFLT_ETN;
    }
    return name;
  }
  
  public static void main(String[] args) {
    System.out.println(getName("bob.1.properties"));
  }
}
