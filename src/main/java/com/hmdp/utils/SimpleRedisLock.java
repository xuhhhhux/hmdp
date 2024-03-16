package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private final String name;
    private final StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    private final String VALUE_PREFIX = UUID.randomUUID().toString(true);
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }


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
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), threadId);
    }

//    @Override
//    public void unLock() {
//        String key = KEY_PREFIX + name;
//        String threadId = VALUE_PREFIX + "-" + Thread.currentThread().getId();
//        String id = stringRedisTemplate.opsForValue().get(key);
//        if (id != null && id.equals(threadId)) {
//            stringRedisTemplate.delete(key);
//        }
//    }
}
