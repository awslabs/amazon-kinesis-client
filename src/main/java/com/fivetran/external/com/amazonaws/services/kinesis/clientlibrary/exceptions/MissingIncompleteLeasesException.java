package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.exceptions;

import java.util.Set;

public class MissingIncompleteLeasesException extends RuntimeException {
   private final Set<String> leases;

    public MissingIncompleteLeasesException(Set<String> leases) {
        super("missing leases: " + String.join(",", leases));
        this.leases = leases;
    }

    public Set<String> getLeases() {
        return leases;
    }
}
