package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();

        if (Boolean.FALSE.equals(isFollow)) {
            LambdaQueryWrapper<Follow> queryWrapper = Wrappers.lambdaQuery(Follow.class)
                    .eq(Follow::getUserId, userId)
                    .eq(Follow::getFollowUserId, followUserId);
            baseMapper.delete(queryWrapper);
        } else {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            baseMapper.insert(follow);
        }
        return Result.ok();
    }

    @Override
    public Result followOrNot(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        LambdaQueryWrapper<Follow> queryWrapper = Wrappers.lambdaQuery(Follow.class)
                .eq(Follow::getUserId, userId)
                .eq(Follow::getFollowUserId, followUserId);
        Follow follow = baseMapper.selectOne(queryWrapper);
        return Result.ok(follow != null);
    }
}
