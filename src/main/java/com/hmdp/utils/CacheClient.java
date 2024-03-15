package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(redisData));
    }

    public  <ID, R> R queryShopPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbOp, Long time, TimeUnit unit) {
        if (id == null) {
            throw new RuntimeException("参数不能为空");
        }
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        if (StrUtil.isNotBlank(json)) {
            return JSON.parseObject(json, type);
        }
        if (json != null) {
            return null;
        }
        R r = dbOp.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(keyPrefix + id, "", time, unit);
            return null;
        }
        stringRedisTemplate.opsForValue().set(keyPrefix + id, JSON.toJSONString(r), time, unit);
        return r;
    }

    public  <ID, R> R queryShopWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbOp, Long time, TimeUnit unit) {
        if (id == null) {
            throw new RuntimeException("参数不能为空");
        }
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        if (StrUtil.isNotBlank(json)) {
            return JSON.parseObject(json, type);
        }
        if (json != null) {
            return null;
        }

        R r = null;
        try {
            boolean lock = tryLock(id);
            if (!lock) {
                Thread.sleep(50);
                return queryShopWithMutex(keyPrefix, id, type, dbOp, time, unit);
            }

            // 获取到锁再次查询缓存，防止再次重建
            json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            if (StrUtil.isNotBlank(json)) {
                return JSON.parseObject(json, type);
            }
            if (json != null) {
                return null;
            }

            r = dbOp.apply(id);
            Thread.sleep(200);

            if (r == null) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, unit);
                return null;
            }
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSON.toJSONString(r), time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(id);
        }
        return r;
    }

    public  <ID, R> R queryShopLogicalTime(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbOp, Long time, TimeUnit unit) {
        if (id == null) {
            throw new RuntimeException("参数不能为空");
        }
        String json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isBlank(json)) {
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            return r;
        }

        boolean isLock = tryLock(id);
        if (isLock) {
            json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            if (StrUtil.isBlank(json)) {
                return null;
            }
            redisData = JSONUtil.toBean(json, RedisData.class);
            data = (JSONObject) redisData.getData();
            r = JSONUtil.toBean(data, type);
            if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
                return r;
            }

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R nr = dbOp.apply(id);
                    this.setWithLogicalExpire(LOCK_SHOP_KEY + id, nr, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(id);
                }
            });
        }
        return r;
    }

    /**
     * 获取锁
     * @param id
     * @return
     */
    private <ID> boolean tryLock(ID id) {
        Boolean ok = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + id, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(ok);
    }

    /**
     * 释放锁
     * @param id
     */
    private <ID> void unLock(ID id) {
        stringRedisTemplate.delete(LOCK_SHOP_KEY + id);
    }
}
