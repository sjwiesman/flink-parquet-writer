package com.ververica.example.util;

import com.ververica.avro.generated.User;

import java.util.SplittableRandom;

/**
 * A simple source that generates users in memory.
 */
public class UserGenerator extends ParallelBaseGenerator<User> {

    public UserGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    protected User randomEvent(SplittableRandom rnd, long id) {
        return User.newBuilder()
                .setFavoriteColor("red")
                .setFavoriteNumber(42)
                .setName("Seth")
                .build();
    }
}
