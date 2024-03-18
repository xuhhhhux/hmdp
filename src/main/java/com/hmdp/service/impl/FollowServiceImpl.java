package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_KEY;

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

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();

        if (Boolean.FALSE.equals(isFollow)) {
            LambdaQueryWrapper<Follow> queryWrapper = Wrappers.lambdaQuery(Follow.class)
                    .eq(Follow::getUserId, userId)
                    .eq(Follow::getFollowUserId, followUserId);
            int count = baseMapper.delete(queryWrapper);
            if (count > 0) {
                stringRedisTemplate.opsForSet().remove(FOLLOW_KEY + userId, followUserId.toString());
            }
        } else {
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            int count = baseMapper.insert(follow);
            if (count > 0) {
                stringRedisTemplate.opsForSet().add(FOLLOW_KEY + userId, followUserId.toString());
            }
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

    @Override
    public Result common(Long commonUserId) {
        Long userId = UserHolder.getUser().getId();
        Set<String> set = stringRedisTemplate.opsForSet().intersect(FOLLOW_KEY + userId, FOLLOW_KEY + commonUserId);
        if (set == null || set.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = set
                .stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        List<UserDTO> userDTOS = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
