package com.kohlschutter.boilerpipe.document;

/**
 * Used to represent structured elements of the html page that will be retained as offset annotations on the document.
 * @author mrglass
 *
 */
public abstract class BPAnnotation implements Cloneable {
    public static final boolean debug = false;
    
    //CONSIDER: tag type? like 'a' or 'h1' or 'b'
    public int start;
    public int end;
    
    public final String localName;
    
    protected BPAnnotation(String localName) {
        this.start = 10000000;
        this.end = -10000000;
        this.localName = localName.toLowerCase();
    }
    
    public boolean isValid() {
        return end > start;
    }

    public void addOffset(int offset) {
        this.start += offset;
        this.end += offset;
    }
    
    public BPAnnotation clone() {
        try {
            return (BPAnnotation)super.clone();
        } catch (CloneNotSupportedException e) {
           throw new Error(e);
        }
    }
}
