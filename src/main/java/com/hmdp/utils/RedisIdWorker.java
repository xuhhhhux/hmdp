package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private final long BEGIN_TIME_STAMP = 1609459200L;
    private final int COUNT = 32;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String key) {
        LocalDateTime now = LocalDateTime.now();
        long epochSecond = now.toEpochSecond(ZoneOffset.UTC);
        long time = epochSecond - BEGIN_TIME_STAMP;
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long num = stringRedisTemplate.opsForValue().increment("inc:" + key + ":" + date);
        if (num == null) {
            throw new RuntimeException("生成ID错误");
        }
        return time << COUNT | num;
    }

    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2021, 1, 1, 0, 0, 0);
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println(epochSecond);
    }
}
