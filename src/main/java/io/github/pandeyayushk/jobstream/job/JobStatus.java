package io.github.pandeyayushk.jobstream.job;


public enum JobStatus {
        PENDING, QUEUED, PROCESSING, COMPLETED,
        FAILED, RETRYING, DEAD;


    public boolean isTerminal(){
        return (this==COMPLETED || this==DEAD);
    }

    public boolean isValidTransition(JobStatus target){
        if(this==PENDING && target==QUEUED)return true;
        if(this==QUEUED&&target==PROCESSING)return true;
        if(this==PROCESSING&&(target==COMPLETED||target==FAILED))return true;
        if(this==FAILED&&(target==RETRYING||target==DEAD))return true;
        if(this==RETRYING&&target==QUEUED)return true;
        if(this==DEAD&&target==QUEUED)return true;
        

        return false;
    }
}

