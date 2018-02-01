package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageFilter;
import org.apache.kafka.common.header.Headers;

import java.util.Collection;
import java.util.regex.Pattern;

public class DefaultMessageFilter<K> implements ExtMessageFilter<K> {
    private final Pattern filterPattern;
    private boolean permitAll = false;

    public DefaultMessageFilter(String sPat) {
        if (null == sPat || sPat.isEmpty() || ".*".equals(sPat) || "*".equals(sPat)) {
            permitAll = true;
        }
        this.filterPattern = Pattern.compile(sPat);
    }

    @Override
    public boolean canDelieveryMessage(ExtMessage<K> message, Headers headers) {
        if (permitAll) {
            return true;
        }

        Collection<String> tags = message.getTags();

        for (String tag : tags) {
            if (filterPattern.matcher(tag).matches()) {
                return true;
            }
        }
        return false;
    }
}
