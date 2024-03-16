package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private final String name;
    private final StringRedisTemplate stringRedisTemplate;
    private final String KEY_PREFIX = "lock:";
    private final String VALUE_PREFIX = UUID.randomUUID().toString(true);


    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long expireTime) {
        String key = KEY_PREFIX + name;
        String threadId = VALUE_PREFIX + "-" + Thread.currentThread().getId();
        Boolean ok = stringRedisTemplate.opsForValue().setIfAbsent(key, threadId, expireTime, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void unLock() {
        String key = KEY_PREFIX + name;
        String threadId = VALUE_PREFIX + "-" + Thread.currentThread().getId();
        String id = stringRedisTemplate.opsForValue().get(key);
        if (id != null && id.equals(threadId)) {
            stringRedisTemplate.delete(key);
        }
    }
}
