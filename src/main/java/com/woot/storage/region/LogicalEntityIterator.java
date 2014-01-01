package com.woot.storage.region;

import com.google.common.collect.AbstractIterator;
import com.woot.storage.Entity;

import java.util.Arrays;
import java.util.Iterator;

public class LogicalEntityIterator extends AbstractIterator<Entity> {

    private final Iterator<Entity> lexicalIterator;
    private Entity previous;

    public LogicalEntityIterator(Iterator<Entity> lexicalIterator) {
        this.lexicalIterator = lexicalIterator;
    }

    @Override
    protected Entity computeNext() {
        if (lexicalIterator.hasNext()) {
            Entity next = lexicalIterator.next();

            if (next.isDeleted()) {
                previous = next;
                return null;
            }

            if (previous != null && Arrays.equals(previous.getKey(), next.getKey())) {
                if (lexicalIterator.hasNext()) {
                    return null;
                } else {
                    return endOfData();
                }
            }
            previous = next;
            return next;

        }
        return endOfData();
    }
}
