package com.commercehub.watershed.pump;

import rx.Producer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Producer which serializes concurrent requests for items, so only one thread at a time actually emits items.
 *
 * Based on https://gist.github.com/benjchristensen/b607a5db4611361149aa#file-reactivepullcold-java-L38
 *
 * @author pmogren
 */
public abstract class SerializingProducer implements Producer {
    private final AtomicLong requested = new AtomicLong();

    /**
     * Add the request but only kick off work if at 0.
     *
     * This is done because over async boundaries `request(n)` can be called multiple times by
     * another thread while this `Producer` is still emitting. We only want one thread ever emitting.
     */
    @Override
    public void request(long request) {
        if (requested.getAndAdd(request) == 0) {
            //noinspection StatementWithEmptyBody
            while (onItemRequested() && requested.decrementAndGet() > 0);
        }
    }

    /**
     * @return whether it is okay to continue requesting items
     */
    protected abstract boolean onItemRequested();
}