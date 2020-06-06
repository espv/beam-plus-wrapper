package no.uio.ifi;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class DolToEur implements SerializableFunction<Double, Double> {
    public DolToEur() {}

    @Override
    public Double apply(Double input) {
        return input * 0.89;
    }
}