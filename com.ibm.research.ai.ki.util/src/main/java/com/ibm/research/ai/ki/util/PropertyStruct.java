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
import java.util.*;

/**
 * For properties, like configuration or hyperparameters that you can serialize in a property file format 
 * but have specific fields, so config.numInstances rather than getProperty("numInstances").
 * All fields should be public, any subclasses of this class should be public.
 * 
 * You can serialize them or use the toString, as long as you define a toStringSpecial / fromStringSpecial
 * for any types that don't have a PropertyEditor, this class already handles basic arrays: int[], float[], double[], String[].
 * Try to have simple fields: ints, doubles, enums, boolean, etc.
 * 
 * For fields you do not want to serialize, begin the name of the field with '_'.
 * 
 * @author mrglass
 *
 */
public class PropertyStruct implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Properties there were no field for. These are not really accessible. We just keep them so we save everything we load.
	 */
	protected final Properties _otherProperties = new Properties();
	
	public void fromProperties(Properties props) {
		for (String k : props.stringPropertyNames()) {
			try {
				Field f = this.getClass().getField(k);
				String strVal = props.getProperty(k);
				setStringValue(f, strVal);
			} catch (NoSuchFieldException nsfe) {
				//fine, not our field
			    _otherProperties.put(k, props.getProperty(k));
			}
		}
	}
	
	public Properties toProperties() {		
		Properties props = new Properties();
		for (Class cls = this.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
			for (Field f : cls.getDeclaredFields()) {
				if (Modifier.isStatic(f.getModifiers()))
					continue;
				if (f.getName().startsWith("_"))
                    continue;
				String val = getStringValue(f);
				props.setProperty(f.getName(), val);		
			}
		}
		for (String k : _otherProperties.stringPropertyNames()) {
		    if (!props.containsKey(k)) {
		        props.put(k, _otherProperties.getProperty(k));
		    }
		}
		return props;		
	}
	
	protected void setStringValue(Field f, String strVal) {
		try {
			if (strVal.equals("null")) {
				f.set(this, null);
				return;
			}
			PropertyEditor pe = PropertyEditorManager.findEditor(f.getType());
			Object val = null;
			if (pe == null) {
				val = fromStringSpecial(f, strVal);
			} else {
				pe.setAsText(strVal);
				val = pe.getValue();
			}
			
			f.set(this, val);
		} catch (IllegalAccessException iae) {
			throw new Error("Set your PropertyStruct fields public: "+f.getName(), iae);
		}
	}

	/**
	 * Get the string value of a field on an object.
	 * @param obj
	 * @param f
	 * @param errorOnFailure enable checking for whether this field can by serialized in properties format
	 * @return
	 */
    public static String getStringValue(Object obj, Field f, boolean errorOnFailure) {
        String fname = null;
        try {
            fname = f.getName();
            PropertyEditor pe = PropertyEditorManager.findEditor(f.getType());
            String val = null;
            if (pe == null) {
                val = toStringSpecialBasic(f, f.get(obj));
            } else {
                pe.setValue(f.get(obj));
                val = pe.getAsText();
            }
            if (val == null)
                return "null";
            if (!errorOnFailure)
                return val;
            if (val.equals("null"))
                throw new Error("Cannot have the actual value of 'null'. In "+fname);
            if (val.indexOf('\n') != -1) 
                throw new Error("Cannot serialize PropertyStruct with newline. In "+fname);
            if (!val.isEmpty() && (Character.isWhitespace(val.charAt(0))||Character.isWhitespace(val.charAt(val.length()-1))))
                throw new Error("Leading or trailing whitespace is not allowed. In "+fname);
            return val;
        } catch (IllegalAccessException iae) {
            if (errorOnFailure) {
                throw new Error("Set your PropertyStruct fields public: " + fname, iae);
            } else {
                return null;
            }
        } catch (NullPointerException e) {
            throw new Error("NPE at " + fname, e);
        }
    }
	
	protected String getStringValue(Field f) {
		String fname = null;
		try {
			fname = f.getName();
			
			PropertyEditor pe = PropertyEditorManager.findEditor(f.getType());
			String val = null;
			if (pe == null) {
				val = toStringSpecial(f, f.get(this));
			} else {
				pe.setValue(f.get(this));
				val = pe.getAsText();
			}
			if (val == null)
				val = "null";
			else if (val.equals("null"))
				throw new Error("Cannot have the actual value of 'null'. In "+fname);
			if (val.indexOf('\n') != -1) 
				throw new Error("Cannot serialize PropertyStruct with newline. In "+fname);
			if (!val.isEmpty() && (Character.isWhitespace(val.charAt(0))||Character.isWhitespace(val.charAt(val.length()-1))))
				throw new Error("Leading or trailing whitespace is not allowed. In "+fname);
			return val;
		} catch (IllegalAccessException iae) {
			throw new Error("Set your PropertyStruct fields public: "+fname, iae);
		} catch (NullPointerException e) {
			throw new Error("NPE at "+fname, e);
		}
	}
	
	public String toString() {
		//toProperties.toString is not as good maybe, since we lose the order
		List<String> lines = new ArrayList<>();
		Set<String> propsSet = new HashSet<>();
		boolean addedOne = false;
		for (Class cls = this.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
		    addedOne = false;
			for (Field f : cls.getDeclaredFields()) {
				if (Modifier.isStatic(f.getModifiers()))
					continue;
				if (f.getName().startsWith("_"))
	                continue;
				if (!addedOne) {
				    lines.add("# from "+cls.getSimpleName());
				}
				addedOne = true;
				String val = getStringValue(f);
				lines.add(f.getName()+" = "+val);
				propsSet.add(f.getName());
			}
		}
		addedOne = false;
		for (String k : _otherProperties.stringPropertyNames()) {
            if (!propsSet.contains(k)) {
                if (!addedOne) {
                    lines.add("# Other properties");
                }
                addedOne = true;
                lines.add(k+" = "+_otherProperties.getProperty(k));
            }
        }

		return Lang.stringList(lines, "\n")+"\n";
	}
	
	/**
	 * override this to string serialize types that don't have a PropertyEditor
	 * int[],double[] are handled in this class
	 * @param f
	 * @param val
	 * @return
	 */
	protected String toStringSpecial(Field f, Object val) {
	    return toStringSpecialBasic(f, val);
	}
	
	static String toStringSpecialBasic(Field f, Object val) {
        if (val == null)
            return null;
        if (f.getType() == int[].class) {
            return Arrays.toString((int[])val);
        } else if (f.getType() == double[].class) {
            return Arrays.toString((double[])val);
        } else if (f.getType() == float[].class) {
            return Arrays.toString((float[])val);
        } else if (f.getType() == String[].class) {
            //check that none of the strings contain "," or trimmable whitespace
            for (String vi : ((String[])val)) {
                if (vi != null && vi.indexOf(',') != -1)
                    throw new IllegalArgumentException("Cannot have ',' in string PropertyStruct when in array");
                if (vi != null && vi.trim().length() != vi.length())
                    throw new IllegalArgumentException("Cannot have leading or trailing whitespace for string in PropertyStruct");
            }
            return Arrays.toString((String[])val);
        }
        throw new UnsupportedOperationException("Please define toStringSpecial for your PropertyStruct extensions "
                + "("+f.getName()+" is "+f.getType()+")");	    
	}
	
	/**
	 * override this to string serialize types that don't have a PropertyEditor
	 * int[],double[] are handled in this class
	 * @param f
	 * @param str
	 * @return
	 */
	protected Object fromStringSpecial(Field f, String str) {
		if (f.getType() == int[].class) {
			if (str.startsWith("[") && str.endsWith("]"))
				str = str.substring(1, str.length()-1).trim();
			if (str.isEmpty())
			    return new int[0];
			String[] ar = str.split(",");
			int[] arin = new int[ar.length];
			for (int i = 0; i < arin.length; ++i)
				arin[i] = Integer.parseInt(ar[i].trim());
			return arin;
		} else if (f.getType() == double[].class) {
			return DenseVectors.fromString(str);
		} else if (f.getType() == float[].class) {
			return DenseVectors.toFloatArray(DenseVectors.fromString(str));
		} else if (f.getType() == String[].class) {
		    if (str.startsWith("[") && str.endsWith("]"))
                str = str.substring(1, str.length()-1).trim();
            if (str.isEmpty())
                return new String[0];
		    String[] ar = str.split(",");
		    for (int i = 0; i < ar.length; ++i)
		        ar[i] = ar[i].trim();
			return ar;
		}
		throw new UnsupportedOperationException("Please define fromStringSpecial for your PropertyStruct extensions "
				+ "("+f.getName()+" is "+f.getType()+")");
	}
	
	public void fromFile(File file) {
	    fromString(FileUtil.readFileAsString(file));
	}
	
	public void fromString(String str) {
		fromProperties(PropertyLoader.fromString(str));
	}
}

