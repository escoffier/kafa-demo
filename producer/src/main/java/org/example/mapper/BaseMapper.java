package org.example.mapper;

import org.apache.ibatis.annotations.Param;
import org.example.util.ExtLimit;

import java.util.List;

public interface BaseMapper<T> {
    /**
     * 插入
     * @param t
     * @return
     */
    Long insert(T t);

    /**
     * 更新
     * @param t
     * @return
     */
    Long updateById(T t);

    /**
     * 按ID删除
     * @param id
     */
    void deleteById(Long id);

    /**
     * 按ID查询
     * @param id
     * @return
     */
    T selectById(Long id);

    /**
     * 按ID查询多个
     * @param ids
     * @return
     */
    List<T> selectByIds(List<Long> ids);

    /**
     * 按实体查询
     * @param t
     * @return
     */
    List<T> selectByEntity(T t);

    /**
     * 查询记录数，可为分页查询
     * @param t
     * @return
     */
    Long selectLimitCount(@Param("ety") T t);

    /**
     * 按分页查询
     * @param t
     * @param limit
     * @return
     */
    List<T> selectByLimit(@Param("ety") T t, @Param("limit") ExtLimit limit);
}
