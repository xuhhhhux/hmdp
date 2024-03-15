package com.hmdp;

import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private ShopServiceImpl shopService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    private final ExecutorService executorService = Executors.newFixedThreadPool(500);

    @Test
    void testRedisIdWorker() throws InterruptedException {
        for (int i = 0; i < 300; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < 100; j++) {
                    long order = redisIdWorker.nextId("order");
                    System.out.println(order);
                }
            });
        }
    }

    @Test
    void saveShop() {
        shopService.saveShopWithExpire(1L, 10L);
    }
}
