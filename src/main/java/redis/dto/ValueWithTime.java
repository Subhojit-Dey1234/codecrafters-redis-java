package redis.dto;

import java.time.Instant;

public class ValueWithTime {
    private final String value;
    private final Instant getTime;
    private final long time;

    public ValueWithTime(String value, Instant getTime, long time) {
        this.value = value;
        this.getTime = getTime;
        this.time = time;
    }

    public String getValue() {return value;}
    public Instant getGetTime() {return getTime;}
    public long getTime() {return time;}
}
