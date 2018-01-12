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

import java.io.Serializable;
import java.util.*;
import java.util.function.*;

import com.google.common.collect.*;


public class Span implements Cloneable, Serializable, Comparable<Span> {
  private static final long serialVersionUID = 1L;

  public static final char INTERVAL_SEP_SYMBOL = ',';

  // start is the first char, end is one past the last char
  public int start, end;

  public Span() {}
  
  public Span(int first, int last) {
    this.start = first;
    this.end = last;
  }
  
  public Span(Span span) {
    this.start = span.start;
    this.end = span.end;
  }

  /**
   * returns the String indicated by the Span from text
   * 
   * @param text
   * @return
   */
  public String substring(String text) {
    return substring(text, 0);
  }

  public String substring(String text, int offset) {
	  return text.substring(this.start+offset, this.end+offset);
  }
	public String substringLimit(String whole) {
		return whole.substring(Math.max(start,0), Math.min(whole.length(),end));
	}  
  
  /**
   * true if the spans overlap at all
   * 
   * @param s
   * @return
   */
  public boolean overlaps(Span s) {
    if (s == null) 
      return false;
    return overlaps(this.start, this.end, s.start, s.end);
  }
  
  public static boolean overlap(Iterable<? extends Span> spans) {
	  List<Span> sl = new ArrayList<>();
	  for (Span s : spans)
		  sl.add(s);
	  Collections.sort(sl);
	  Span prev = null;
	  for (Span s : sl) {
		  if (prev != null && prev.overlaps(s))
			  return true;
		  prev = s;
	  }
	  return false;
  }

  public static Span coveringSpan(Span... spans) {
	  return coveringSpan(Arrays.asList(spans));
  }
  public static Span coveringSpan(Iterable<? extends Span> spans) {
	  int minStart = Integer.MAX_VALUE;
	  int maxEnd = Integer.MIN_VALUE;
	  for (Span s : spans) {
		  minStart = Math.min(minStart, s.start);
		  maxEnd = Math.max(maxEnd, s.end);
	  }
	  return new Span(minStart, maxEnd);
  }
  
  /**
   * true if the spans overlap at all
   * 
   * @param s
   * @return
   */
  public boolean overlaps(int s, int e) {
	  return overlaps(this.start, this.end, s, e);
  }
  
  public static boolean overlaps(int s1, int e1, int s2, int e2) {
	  return s1 < e2 && s2 < e1;
  }

  public static boolean contains(int s1, int e1, int s2, int e2) {
	  return (s1 <= s2 && e1 >= e2);
  }
  
  /**
   * true if this span contains (or is equal to) s
   * 
   * @param s
   * @return boolean
   */
  public boolean contains(Span s) {
    return (start <= s.start && end >= s.end);
  }

	public boolean contains(int ndx) {
		return ndx >= start && ndx < end;
	}
	public boolean contains(int otherStart, int otherEnd) {
		return (start <= otherStart && end >= otherEnd);
	}
	public boolean properContains(Span s) {
		return (start <= s.start && end >= s.end && 
				(start < s.start || end > s.end));
	}
	
	/**
	 * neither span contains the other, but they do overlap
	 * @param s
	 * @return
	 */
	public boolean crosses(Span s) {
		return start < s.start && end < s.end && end > s.start ||
				s.start < start && s.end < end && s.end > start;
				
	}
	
  /**
   * the span in interval notation http://en.wikipedia.org/wiki/Interval_(mathematics)
   */
  @Override
  public String toString() {
    return toString(this);
  }

  public static String toString(Span span) {
	  return ("[" + span.start + INTERVAL_SEP_SYMBOL + span.end + ")");
  }
  
  /**
   * creates a span from interval notation
   * 
   * @param str
   * @return span
   */
  public static Span fromString(String str) {
    str = str.trim();
    int sadj = 0;
    if (str.startsWith("("))
    	sadj = 1;
    else if (str.startsWith("["))
    	sadj = 0;
    else
    	throw new IllegalArgumentException("bad span string: "+str);
    int eadj = 0;
    if (str.endsWith(")"))
    	eadj = 0;
    else if (str.endsWith("]"))
    	eadj = 1;
    else
    	throw new IllegalArgumentException("bad span string: "+str);
    int mid = str.indexOf(INTERVAL_SEP_SYMBOL);
    String begin = str.substring(1, mid).trim();
    String end = str.substring(mid + 1, str.length()-1).trim();
    try {
    	return new Span(Integer.parseInt(begin)+sadj, Integer.parseInt(end)+eadj);
    } catch (NumberFormatException nfe) {
    	throw new IllegalArgumentException("bad span string: "+str);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + start;
    result = prime * result + end;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Span other = (Span) obj;
    if (start != other.start)
      return false;
    if (end != other.end)
      return false;

    return true;
  }
  
	public static class LengthComparator implements Comparator<Span> {

		@Override
		public int compare(Span o1, Span o2) {
			return o1.length() - o2.length();
		}
		
	}
	
	public static int sumLength(Span s1, Span s2) {
		int overlapLen = s1.overlapLength(s2);
		return s1.length() + s2.length() - overlapLen;
	}
	
	public int distanceTo(Span s) {
		if (s.start > end)
			return s.start - end;
		if (start > s.end)
			return start - s.end;
		return 0;
	}
	
	public boolean fix(int minStart, int maxEnd) {
		boolean changed = false;
		if (start < minStart) {
			start = minStart; 
			changed = true;
		}
		if (end > maxEnd) {
			end = maxEnd;
			changed = true;
		}
		return changed;
	}
	
	/**
	 * Assumes the spans are non-overlapping
	 * @param source
	 * @param replacements
	 * @return
	 */
	public static String replaceAll(String source, Map<Span,String> replacements) {
		ArrayList<Pair<Span,String>> rep = HashMapUtil.toPairs(replacements);
		Collections.sort(rep, new FirstPairComparator());
		StringBuffer buf = new StringBuffer();
		int leftOff = 0;
		for (Pair<Span,String> rp : rep) {
			if (rp.first.start >= leftOff) {
				try {
					buf.append(source.substring(leftOff,rp.first.start));
				} catch (Exception e) {
					System.out.println(source+" "+leftOff+", "+rp.first.start);
				}
				buf.append(rp.second);
				leftOff = rp.first.end;
			}
		}
		buf.append(source.substring(leftOff));
		return buf.toString();
	}
	
	/**
	 * Shift this span by the provided offset
	 * @param offset
	 */
	public void addOffset(int offset) {
		start += offset;
		end += offset;
	}

	//CONSIDER: whichSegment, snapToSegmentation and toSegmentSpan could benefit from a datastructure that does binary searches over lists of sorted starts and ends
	
	/**
	 * Finds the index of the segment that completely contains this span, or -1 if no such span exists
	 * @param segments assumed sorted and non-overlapping
	 * @return
	 */
	public int whichSegment(List<? extends Span> segments) {
		int prevEnd = 0;
		for (int i = 0; i < segments.size(); ++i) {
			//check sorted and non-overlapping
			if (segments.get(i).start < prevEnd) {
				throw new IllegalArgumentException("segments must be sorted and non-overlapping");
			}
			prevEnd = segments.get(i).end;
			if (segments.get(i).contains(this))
				return i;
		}
		return -1;
	}
	
	/**
	 * snaps this span to begin on a toks start and end on a toks end
	 * @param toks list of spans that are a segmentation of some sequence, these must be sorted and non-overlapping
	 * @return overlap percent with original span
	 */
	public double snapToSegmentation(List<? extends Span> toks) {
		boolean setStart = false;
		int prevEnd = 0;
		int origStart = this.start;
		int origEnd = this.end;
		for (int i = 0; i < toks.size(); ++i) {
			//check sorted and non-overlapping
			if (toks.get(i).start < prevEnd)
				throw new IllegalArgumentException("toks must be sorted and non-overlapping");
			prevEnd = toks.get(i).end;
			
			if (!setStart && toks.get(i).start >= this.start) {
				//closer to previous or current?
				if (i > 0 && Math.abs(toks.get(i).start - this.start) > Math.abs(toks.get(i-1).start - this.start))
					this.start = toks.get(i-1).start;
				else
					this.start = toks.get(i).start;
				setStart = true;
			} else if (!setStart && i == toks.size()-1) {
				this.start = toks.get(i).start;
				setStart = true;
			}
			if (setStart && toks.get(i).end >= this.end) {
				//closer to previous or current?
				if (i > 0 && Math.abs(toks.get(i).end - this.end) > Math.abs(toks.get(i-1).end - this.end) && toks.get(i-1).end > this.start)
					this.end = toks.get(i-1).end;
				else
					this.end = toks.get(i).end;
				break;
			} else if (setStart && i == toks.size()-1) {
				this.end = toks.get(i).end;
			}
		}
		return this.overlapPercent(origStart, origEnd);
		//if (!this.overlaps(origStart, origEnd)) {
		//	throw new IllegalArgumentException("No span for provided segmentation that overlaps this span.");
		//}
	}
	/**
	 * Translates a span in characters to a span over the provided tokens.
	 * It will attempt to find the closest match, even if there is no i 
	 * s.t. segments.get(i).start == charSpan.start or no j s.t. segments.get(j).end == charSpan.end.
	 * @param charSpan
	 * @param segments assumed sorted
	 * @return a span useable like segments.subList(span.start, span.end), or null if there is no segment span that overlaps the given charSpan
	 */
	public static Span toSegmentSpan(Span charSpan, List<? extends Span> segments) {
		return toSegmentSpan(charSpan, segments, true);
	}
	public static Span toSegmentSpan(Span charSpan, List<? extends Span> segments, boolean forceNonEmpty) {
		if (segments.isEmpty())
			throw new IllegalArgumentException("segments must be non-empty");
		Span segSpan = new Span(-1,-1);
		
		boolean setStart = false;
		int prevEnd = 0;
		for (int i = 0; i < segments.size(); ++i) {
			//check sorted and non-overlapping
			if (segments.get(i).start < prevEnd) {
				throw new IllegalArgumentException("segments must be sorted and non-overlapping");
			}
			prevEnd = segments.get(i).end;
			
			if (!setStart && segments.get(i).start >= charSpan.start) {
				if (i > 0 && Math.abs(segments.get(i).start - charSpan.start) > Math.abs(segments.get(i-1).start - charSpan.start))
					segSpan.start = i-1;
				else
					segSpan.start = i;
				setStart = true;
			} else if (!setStart && i == segments.size()-1) {
				segSpan.start = i;
				setStart = true;
			}
			if (setStart && segments.get(i).end >= charSpan.end) {
				if (i > 0 && Math.abs(segments.get(i).end - charSpan.end) > Math.abs(segments.get(i-1).end - charSpan.end) && segments.get(i-1).end > charSpan.start)
					segSpan.end = i;
				else
					segSpan.end = i+1;
				break;
			} else if (setStart && i == segments.size()-1) {
				segSpan.end = i + 1;
			}
		}
		//force the segment span to be non-empty
		if (forceNonEmpty && segSpan.length() == 0) {
			if (segSpan.start == 0)
				++segSpan.end;
			else if (segSpan.end == segments.size())
				--segSpan.start;
			else if (charSpan.overlapLength(segments.get(segSpan.start-1).start, segments.get(segSpan.end-1).end) > 
					 charSpan.overlapLength(segments.get(segSpan.start).start, segments.get(segSpan.end).end)) 
				--segSpan.start; //fit to the token that has the highest overlap
			else
				++segSpan.end;
		}
		//verify segment span at least overlaps the charSpan
		if (!charSpan.overlaps(segments.get(segSpan.start).start, segments.get(segSpan.end-1).end))
			return null;
		return segSpan;
	}
	
	/**
	 * 
	 * @param spans character level spans
	 * @param segments must be sorted, should be non-overlapping
	 * @return original spans with start and end in terms of segment - snaps to closest segmentation
	 */
	public static List<Span> toSegmentSpans(Iterable<? extends Span> spans, List<? extends Span> segments) {
		List<Span> segSpans = new ArrayList<>();
		for (Span charSpan : spans) {
			segSpans.add(toSegmentSpan(charSpan, segments));
		}
		return segSpans;
	}
	
	public static ArrayList<Span> complement(List<Span> spans, int from, int to) {
		ArrayList<Span> comp = new ArrayList<Span>();
		Collections.sort(spans);
		int prevEnd = from;
		for (Span s : spans) {
			if (s.start > prevEnd)
				comp.add(new Span(prevEnd, s.start));
			prevEnd = s.end+1;
		}
		if (to > prevEnd)
			spans.add(new Span(prevEnd, to));
		return comp;
	}
	
	public static int firstStart(Iterable<? extends Span> spans) {
		int start = Integer.MAX_VALUE;
		for (Span s : spans)
			start = Math.min(s.start, start);
		return start;
	}
	
	public static int lastEnd(Iterable<? extends Span> spans) {
		int end = Integer.MIN_VALUE;
		for (Span s : spans)
			end = Math.max(s.end, end);
		return end;
	}
	
	/**
	 * only makes sense if the spans don't have start or end within whitespace
	 * @deprecated use OffsetCorrection for this
	 * @param source
	 * @param spans
	 * @return
	 */
	public static String compressSpace(String source, Collection<Span> spans) {
		StringBuffer buf = new StringBuffer();
		boolean lastWasSpace = false;
		for (int i = 0; i < source.length(); ++i) {
			if (Character.isWhitespace(source.charAt(i))) {
				if (lastWasSpace) {
					//decrease all offsets after current position by one
					for (Span s : spans) {
						if (s.end >= buf.length()) {
							--s.end;
							if (s.start >= buf.length()) {
								--s.start;
							}
						}
					}
				}
				lastWasSpace = true;
			} else {
				if (lastWasSpace) {
					buf.append(' ');
				}
				buf.append(source.charAt(i));
				lastWasSpace = false;
			}
		}
		return buf.toString();
	}
	
	public static String highlight(String source, Span span, String begin, String end) {
		return highlight(source, span, begin, end, 0);
	}
	
	public static String highlight(String source, Span span, String begin, String end, int offset) {
		return source.substring(0,span.start+offset) + begin + span.substring(source, offset) + end + source.substring(span.end+offset);
	}
	
	public static String highlightAll(String source, List<? extends Span> spans, String begin, String end) {
		return highlightAll(source, spans, begin, end, 0);
	}
	
	/**
	 * The spans are adjusted by offset.
	 * So if source is a substring of the string that 'spans' reference the offset should be the negative of the starting index.
	 * @param source
	 * @param spans
	 * @param begin
	 * @param end
	 * @param offset
	 * @return
	 */
	public static String highlightAll(String source, List<? extends Span> spans, String begin, String end, int offset) {
		if (!Ordering.natural().isOrdered(spans)) {
			spans = new ArrayList<Span>(spans);
			Collections.sort(spans);
		}
		StringBuffer buf = new StringBuffer();
		int leftOff = 0;
		for (Span span : spans) {
			if (span.start+offset < 0 || span.end+offset > source.length()) {
				continue;
			}
			if (span.start+offset >= leftOff) {
				try {
					buf.append(source.substring(leftOff,span.start+offset));
				} catch (Exception e) {
					System.out.println(source+" "+leftOff+", "+span.start+offset);
				}
				buf.append(begin);
				buf.append(span.substring(source, offset));
				buf.append(end);
				leftOff = span.end+offset;
			}
		}
		if (leftOff < source.length())
			buf.append(source.substring(leftOff));
		return buf.toString();
	}
	
	public static <T extends Span,X extends T> String highlightAll(String source, List<X> spans, Function<T,Pair<String,String>> beginEndMaker) {
		return highlightAll(source, spans, beginEndMaker, 0);
	}
	
	public static <T extends Span,X extends T> String highlightAll(String source, List<X> spans, Function<T,Pair<String,String>> beginEndMaker, int offset) {
		if (!Ordering.natural().isOrdered(spans)) {
			spans = new ArrayList<X>(spans);
			Collections.sort(spans);
		}
		StringBuffer buf = new StringBuffer();
		int leftOff = 0;
		for (T span : spans) {
			if (span.start+offset < 0 || span.end+offset > source.length()) {
				continue;
			}
			if (span.start+offset >= leftOff) {
				try {
					buf.append(source.substring(leftOff,span.start+offset));
				} catch (Exception e) {
					System.out.println(source+" "+leftOff+", "+span.start+offset);
				}
				Pair<String,String> be = beginEndMaker.apply(span);
				buf.append(be.first);
				buf.append(span.substring(source, offset));
				buf.append(be.second);
				leftOff = span.end+offset;
			}
		}
		if (leftOff < source.length())
			buf.append(source.substring(leftOff));
		return buf.toString();
	}
	
	public Pair<String,List<Span>> inverseHighlight(String highlighted, String start, String end) {
		int currentPos = -1;
		int startPos = 0;
		StringBuilder buf = new StringBuilder();
		List<Span> spans = new ArrayList<Span>();
		while ((startPos = highlighted.indexOf(start, currentPos+1)) != -1) {
			int endPos = highlighted.indexOf(end, startPos+start.length());
			int startNdx = buf.length();
			buf.append(highlighted.substring(currentPos, startPos));
			buf.append(highlighted.substring(startPos+start.length(), endPos));
			currentPos = endPos + end.length();
			spans.add(new Span(startNdx, buf.length()));
		}
		return Pair.of(buf.toString(), spans);
	}
	
	public boolean isValid(String str) {
		return start <= end && start >= 0 && end <= str.length();
	}	
	
	public Span clone() {
		try {
		return (Span)super.clone();
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	public int overlapLength(int otherStart, int otherEnd) {
		return Math.max(0, 
				Math.min(this.end, otherEnd) - Math.max(this.start, otherStart));
	}
	
	public int overlapLength(Span other) {
		return Math.max(0, 
				Math.min(this.end, other.end) - Math.max(this.start, other.start));
	}
	
	public double overlapPercent(int otherStart, int otherEnd) {
		return Math.max(0.0, 
				Math.min(this.end, otherEnd) - Math.max(this.start, otherStart)) / 
				(Math.max(this.end, otherEnd) - Math.min(this.start, otherStart));
	}
	
	public double overlapPercent(Span other) {
			return (double)overlapLength(other) / 
					(Math.max(this.end, other.end) - Math.min(this.start, other.start));
	}
	
	public int length() {
		return end - start;
	}	
	
	public int compareTo(Span s) {
		if (start == s.start) {
			return s.end - end; //longer spans first
		}
		return start - s.start;
	}
}
