package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.queryBlogIsLike(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlog(Long id) {
        Blog blog = baseMapper.selectById(id);
        if (blog == null) {
            return Result.fail("商铺不存在");
        }

        queryBlogUser(blog);
        queryBlogIsLike(blog);
        return Result.ok(blog);
    }

    private void queryBlogIsLike(Blog blog) {
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        Boolean ok = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        blog.setIsLike(Boolean.TRUE.equals(ok));
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + id;

        Boolean ok = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        if (Boolean.FALSE.equals(ok)) {
            boolean cnt = update().setSql("liked = liked + 1").eq("id", id).update();
            if (cnt) {
                stringRedisTemplate.opsForSet().add(key, userId.toString());
            }
        } else {
            boolean cnt = update().setSql("liked = liked - 1").eq("id", id).update();
            if (cnt) {
                stringRedisTemplate.opsForSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

}
