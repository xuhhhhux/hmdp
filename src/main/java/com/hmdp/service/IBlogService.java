package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    Result queryBlog(Long id);

    Result queryHotBlog(Integer current);

    Result likeBlog(Long id);

    Result queryLikes(Long id);

    Result ofUser(Long id, Integer current);

    Result queryBlogOfFollow(Long max, Integer offset);
}
