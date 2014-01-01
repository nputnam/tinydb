package com.woot.storage.region;

import com.google.common.primitives.SignedBytes;
import com.woot.storage.Entity;

import java.util.Comparator;

public class EnityComparator implements Comparator<Entity> {

    @Override
    public int compare(Entity o1, Entity o2) {
        int lexCompare = SignedBytes.lexicographicalComparator().compare(o1.getKey(), o2.getKey());
        // Same key return the most recent one.
        if (lexCompare == 0) {
            return (o1.getTimestamp().compareTo(o2.getTimestamp())) * -1;
        }
        else {
            return lexCompare;
        }
    }

}
