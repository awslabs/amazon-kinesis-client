package software.amazon.kinesis.common;

import java.util.function.Supplier;

import lombok.RequiredArgsConstructor;

/**
 * Caches results from a {@link Supplier}. Caching is especially useful when
 * {@link Supplier#get()} is an expensive call that produces static results.
 */
@RequiredArgsConstructor
public class SupplierCache<T> {

    private final Supplier<T> supplier;

    private volatile T result;

    /**
     * Returns the cached result. If the cache is null, the supplier will be
     * invoked to populate the cache.
     *
     * @return cached result which may be null
     */
    public T get() {
        if (result == null) {
            synchronized (this) {
                // double-check lock
                if (result == null) {
                    result = supplier.get();
                }
            }
        }
        return result;
    }

}
