package com.hmdp.utils;

public interface ILock {
    boolean tryLock(long expireTime);
    void unLock();
}
