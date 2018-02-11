package org.apache.kafka.clients.enhance.consumer;

import java.util.NoSuchElementException;

/**
 * Created by steven03.zhang on 2018/1/6.
 */
public enum ConsumeModel {
    GROUP_NULL_MODEL(-1, "NullModel"),
    GROUP_CLUSTERING(0, "Clustering"),
    GROUP_BROADCASTING(1, "Broadcasting");

    public final int id;
    public final String name;

    ConsumeModel(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static ConsumeModel forName(String name) {
        for (ConsumeModel t : values())
            if (t.name.equals(name))
                return t;
        throw new NoSuchElementException("Invalid Consumer Model " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
