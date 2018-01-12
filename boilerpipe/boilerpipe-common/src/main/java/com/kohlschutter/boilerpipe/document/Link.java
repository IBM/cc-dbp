package com.kohlschutter.boilerpipe.document;

/**
 * HTML anchor tag as offset annotation
 * @author mrglass
 *
 */
public class Link extends BPAnnotation {
    public String href;
    
    public Link(String href) {
        super("a");
        this.href = href;
    }
    
    public boolean isValid() {
        return start < end && href != null;
    }
}
