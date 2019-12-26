package me.ele.jarch.athena.util.etrace;

import me.ele.jarch.athena.util.NoThrow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

public abstract class ImpatientMetricsSender {
    private static final Logger logger = LoggerFactory.getLogger(ImpatientMetricsSender.class);

    public static final Map<String, ImpatientMetricsSender> senders = new ConcurrentHashMap<>();

    private final ImpatientMetricsSender successor;

    private static final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(20_000);
    private static final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, queue,
            r -> new Thread(r, "ImpatientMetricsSenderWorker"),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    public ImpatientMetricsSender(ImpatientMetricsSender successor) {
        this.successor = successor;
    }


    public static ImpatientMetricsSender getSender(String type,
        Supplier<ImpatientMetricsSender> senderInitializer) {
        ImpatientMetricsSender sender = ImpatientMetricsSender.senders.get(type);
        if (sender != null) {
            return sender;
        }

        synchronized (ImpatientMetricsSender.class) {
            if (senders.size() > 1000) {
                senders.clear();
            }
            sender = ImpatientMetricsSender.senders.get(type);
            if (sender != null) {
                return sender;
            }
            try {
                ImpatientMetricsSender newSender = senderInitializer.get();
                ImpatientMetricsSender.senders.put(type, newSender);
                return newSender;
            } catch (Exception e) {
                logger.error("Exception caught when trying to get sender.", e);
            }
            return ImmediateSender.getImmediateSender();
        }
    }

    public final void trySend(Runnable sendAction) {
        executor.execute(() -> {
            NoThrow.call(() -> {
                if (!this.canSend()) {
                    return;
                }
                if (this.successor == null) {
                    sendAction.run();
                    return;
                }
                this.successor.trySend(sendAction);
            });
        });
    }

    protected abstract boolean canSend();

}
