package org.elasticsoftware.elasticactors.tracing;

import java.util.Objects;
import java.util.StringJoiner;

public final class RealSenderData {

    private final String realSender;
    private final String realSenderType;

    public RealSenderData(String realSender, String realSenderType) {
        this.realSender = realSender;
        this.realSenderType = realSenderType;
    }

    public boolean isEmpty() {
        return (this.realSender == null || this.realSender.isEmpty())
                && (this.realSenderType == null || this.realSenderType.isEmpty());
    }

    public String getRealSender() {
        return realSender;
    }

    public String getRealSenderType() {
        return realSenderType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RealSenderData)) {
            return false;
        }

        RealSenderData that = (RealSenderData) o;

        if (!Objects.equals(realSender, that.realSender)) {
            return false;
        }
        return Objects.equals(realSenderType, that.realSenderType);
    }

    @Override
    public int hashCode() {
        int result = realSender != null ? realSender.hashCode() : 0;
        result = 31 * result + (realSenderType != null ? realSenderType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RealSenderData.class.getSimpleName() + "[", "]")
                .add("realSender='" + realSender + "'")
                .add("realSenderType='" + realSenderType + "'")
                .toString();
    }
}
