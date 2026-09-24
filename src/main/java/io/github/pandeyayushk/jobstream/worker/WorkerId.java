package io.github.pandeyayushk.jobstream.worker;

import java.util.Objects;
import java.util.UUID;

public record WorkerId(UUID value) {
    public WorkerId{
        Objects.requireNonNull(value,"WorkerID value can not be null");
    }
    public static WorkerId generate(){
        return new WorkerId(UUID.randomUUID());
    }
    public static WorkerId fromString(String id){
        return new WorkerId(UUID.fromString(id));
    }
    @Override
    public String toString() {
        return value.toString();
    }
}
