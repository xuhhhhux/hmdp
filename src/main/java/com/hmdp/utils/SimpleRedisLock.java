package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private final String name;
    private final StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private final String KEY_PREFIX = "lock:";

    @Override
    public boolean tryLock(long expireTime) {
        long threadId = Thread.currentThread().getId();
        String key = KEY_PREFIX + name;
        Boolean ok = stringRedisTemplate.opsForValue().setIfAbsent(key, threadId + "", expireTime, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void unLock() {
        String key = KEY_PREFIX + name;
        stringRedisTemplate.delete(key);
    }
}
