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
package com.ibm.reseach.ai.ki.nlp.types;

import java.util.*;
import java.util.stream.*;

import javax.xml.parsers.*;

import org.apache.commons.io.*;

import com.fasterxml.jackson.annotation.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.util.*;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;

/**
 * Used to convert an xml document into offset annotation.
 * The text of Document will be the getTextContent of the xml.
 * Each xml tag will be an XmlTag annotation in the Document.
 * @author mrglass
 *
 */
public class XmlTag extends Annotation {
	private static final long serialVersionUID = 1L;
	
	public String name;
	public Map<String,String> attributes;
	protected List<AnnoRef<XmlTag>> children;
	protected AnnoRef<XmlTag> parent;
	
	public XmlTag(@JsonProperty("source") String source, @JsonProperty("start") int start, @JsonProperty("end") int end, 
			@JsonProperty("name") String name, @JsonProperty("attributes") Map<String,String> attributes) {
		super(source, start, end);
		this.name = name;
		this.attributes = attributes;
		this.children = new ArrayList<>();
	}

	public void addChild(Document doc, XmlTag child) {
		children.add(doc.getAnnoRef(child));
		child.parent = doc.getAnnoRef(this);
	}
	
	public XmlTag getParent() {
		return parent == null ? null : parent.get();
	}
	
	public List<XmlTag> getChildren() {
		return children.stream().map(x -> x.get()).collect(Collectors.toList());
	}
	
	public static final String SOURCE = XmlTag.class.getSimpleName();
	
	public static Document xmlToOffsetAnnotation(Node xmldoc, boolean includeRoot) {
		Document doc = new Document(xmldoc.getTextContent());
		StringBuilder buf = new StringBuilder();
		if (includeRoot) {
			convert(xmldoc, buf, doc);		
		} else {
			NodeList nl = xmldoc.getChildNodes();
			for (int i = 0; i < nl.getLength(); ++i) {
				convert(nl.item(i), buf, doc);
			}
		}
		
		if (!doc.text.equals(buf.toString())) {
			throw new Error("text construction failed");
		}
		return doc;
	}
	
	public static Document xmlToOffsetAnnotation(String xml, boolean includeRoot) {
		try {
			//CONSIDER: reuse these?
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		    //factory.setNamespaceAware(true);
			DocumentBuilder builder = factory.newDocumentBuilder();
			
			org.w3c.dom.Document xmlDoc = builder.parse(IOUtils.toInputStream(xml));
			return xmlToOffsetAnnotation(xmlDoc.getDocumentElement(), includeRoot);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}

	static XmlTag convert(Node node, StringBuilder buf, Document doc) {
		if (node.getNodeType() == Node.TEXT_NODE) {
			buf.append(node.getTextContent());
			return null;
		} else if (node.getChildNodes().getLength() == 0) {
			int start = buf.length();
			buf.append(node.getTextContent());
			int end = buf.length();
			XmlTag t = new XmlTag(SOURCE, start, end, node.getNodeName(), getAttributes(node));
			doc.addAnnotation(t);
			return t;
		} else {
			int start = buf.length();
			NodeList nl = node.getChildNodes();
			List<XmlTag> children = new ArrayList<>();
			for (int i = 0; i < nl.getLength(); ++i) {
				Node c = nl.item(i);
				XmlTag t = convert(c, buf, doc);
				if (t != null) {
					children.add(t);
				}
			}
			int end = buf.length();
			XmlTag t = new XmlTag(SOURCE, start, end, node.getNodeName(), getAttributes(node));
			for (XmlTag child : children)
				t.addChild(doc, child);
			doc.addAnnotation(t);
			return t;
		}
	}
	
	static Map<String,String> getAttributes(Node node) {
		Map<String,String> as = new HashMap<>();
		NamedNodeMap attrs = node.getAttributes();
		if (attrs == null)
			return as;
		for (int ai = 0; ai < attrs.getLength(); ++ai) {
			Node attr = attrs.item(ai);
			as.put(attr.getNodeName(), attr.getNodeValue());
		}
		return as;
	}
	
	/*
	private static String testxml = 
		      "<p>" +
		      "  <media type=\"audio\" id=\"au008093\" rights=\"wbowned\"/>" +
		      "    <title>Bee buzz</title>\n" +
		      "\n" +
		      "  Most other kinds of bees live alone instead of in a colony.\n" +
		      "  These bees make tunnels in wood or in the ground.\n" +
		      "  The queen makes her own nest.\n" +
		      "</p>";
	
	private static void toytest() throws Exception {

		DocumentBuilder builder;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    //factory.setNamespaceAware(true);
	    builder = factory.newDocumentBuilder();
		org.w3c.dom.Document xmlDoc = builder.parse(IOUtils.toInputStream(testxml));
		
		org.w3c.dom.Node rootElement = xmlDoc.getDocumentElement();
		System.out.println(rootElement.getTextContent());
		NodeList nl = rootElement.getChildNodes();
		for (int i = 0; i < nl.getLength(); ++i) {
			Node c = nl.item(i);
			System.out.println(i+"ATTR:\n"+HashMapUtil.toString(getAttributes(c)));
			System.out.println(i+"text:"+c.getTextContent());
			System.out.println(i+"name:"+c.getNodeName());
			System.out.println(i+"nval:"+c.getNodeValue());
			System.out.println();
			//c.getNodeType() Node.TEXT_NODE
		}		
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(testxml);
		System.out.println("------------------------");
		System.out.println(xmlToOffsetAnnotation(testxml, true).toSimpleInlineMarkup());
	}
	*/
}
