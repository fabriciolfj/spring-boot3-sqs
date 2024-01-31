package com.github.fabriciolfj.examplesqs;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class UserRepository {

    private final Map<String, User> persistedUsers = new ConcurrentHashMap<>();

    public void save(final User user) {
        persistedUsers.put(user.id(), user);
    }

    public Optional<User> findById(final String id) {
        return Optional.ofNullable(persistedUsers.computeIfAbsent(id, u -> null));
    }

    public Optional<User> findByName(final String name) {
        return persistedUsers.values().stream()
                .filter(u ->  u.name().equals(name))
                .findFirst();
    }
}
