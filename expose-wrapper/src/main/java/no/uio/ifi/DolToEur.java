package no.uio.ifi;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class DolToEur implements SerializableFunction<Long, Long> {
    public DolToEur() {}

    @Override
    public Long apply(Long input) {
        return (long) (input * 0.89);
    }
}