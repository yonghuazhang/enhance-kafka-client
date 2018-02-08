package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread extends KafkaThread {
    private final static Logger logger = LoggerFactory.getLogger(ShutdownableThread.class);
    private final boolean isInterruptible;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public ShutdownableThread(String name, boolean daemon, boolean isInterruptible) {
        super(name, daemon);
        this.isInterruptible = isInterruptible;
    }

    public ShutdownableThread(String name, boolean daemon) {
        this(name, daemon, true);
    }

    public ShutdownableThread(String name) {
        this(name, false, true);
    }

    public ShutdownableThread(String name, Runnable runnable, boolean daemon, boolean isInterruptible) {
        super(name, runnable, daemon);
        this.isInterruptible = isInterruptible;
    }

    public ShutdownableThread(String name, Runnable runnable, boolean daemon) {
        this(name, runnable, daemon, true);
    }

    public void shutdown() {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean initiateShutdown() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Shutting down thread.");
            if (isInterruptible) {
                interrupt();
            }
            return true;
        }
        return false;
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.warn("ShutdownableThread awaitShutdown Error.", e);
        }
        logger.info("Shutdown completed.");
    }

    public abstract void doWork();

    @Override
    public void run() {
        logger.info("Starting thread...");

        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (FatalExitError e) {
            isRunning.set(false);
            shutdownLatch.countDown();
            logger.info("Stopped.");
            Exit.exit(e.statusCode());
        } catch (Throwable e) {
            if (isRunning.get()) {
                logger.error("Error due to", e);
            }
        }

        shutdownLatch.countDown();
        logger.info("Stopped.");
    }
}
