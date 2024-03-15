package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryShopById(Long id) {
        Shop shop = queryShopWithMutex(id);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 缓存穿透
     * @param id
     * @return
     */
    private Shop queryShopPassThrough(Long id) {
        if (id == null) {
            throw new RuntimeException("id不能为空");
        }
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSON.parseObject(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null;
        }
        Shop shop = baseMapper.selectById(id);
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSON.toJSONString(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     * 缓存击穿
     * @param id
     * @return
     */
    @SneakyThrows
    private Shop queryShopWithMutex(Long id) {
        if (id == null) {
            throw new RuntimeException("id不能为空");
        }
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSON.parseObject(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null;
        }

        Shop shop = null;
        try {
            boolean lock = tryLock(id);
            if (!lock) {
                Thread.sleep(50);
                return queryShopWithMutex(id);
            }

            // 获取到锁再次查询缓存，防止再次重建
            shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSON.parseObject(shopJson, Shop.class);
            }
            if (shopJson != null) {
                return null;
            }

            shop = baseMapper.selectById(id);
            Thread.sleep(200);

            if (shop == null) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSON.toJSONString(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(id);
        }
        return shop;
    }

    /**
     * 获取锁
     * @param id
     * @return
     */
    private boolean tryLock(Long id) {
        Boolean ok = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + id, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(ok);
    }

    /**
     * 释放锁
     * @param id
     */
    private void unLock(Long id) {
        stringRedisTemplate.delete(LOCK_SHOP_KEY + id);
    }

    @Override
    @Transactional
    public void update(Shop shop) {
        if (shop == null || shop.getId() == null) {
            throw new RuntimeException("参数不能为空");
        }
        baseMapper.updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
    }
}
