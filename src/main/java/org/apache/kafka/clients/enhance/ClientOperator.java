package org.apache.kafka.clients.enhance;

import java.util.concurrent.TimeUnit;

public interface ClientOperator {

    String clientId();

    void start();

    void shutdownNow();

    void shutdown(long timeout, TimeUnit unit);

    void suspend();

    void resume();

}
