package sulhoe.aura.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import sulhoe.aura.entity.UserTypeKeyword;

import java.util.List;

public interface UserTypeKeywordRepository extends JpaRepository<UserTypeKeyword, Long> {

    /** 제목이 가입자가 type에 구독한 '임의의' 키워드(전역/개인 불문)에 매칭되면 해당 user_id 반환 (OR 매칭) */
    @Query(value = """
        SELECT DISTINCT utk.user_id
        FROM user_type_keywords utk
        JOIN keywords k ON k.id = utk.keyword_id
        WHERE utk.type = :type
          AND LOWER(:title) LIKE CONCAT('%', LOWER(k.phrase), '%')
        """, nativeQuery = true)
    List<Long> findUserIdsByTypeAndTitleMatch(@Param("type") String type,
                                              @Param("title") String title);

    @Query("select utk from UserTypeKeyword utk where utk.user.id = :userId and utk.type = :type")
    List<UserTypeKeyword> findAllByUserAndType(@Param("userId") Long userId, @Param("type") String type);

    boolean existsByUser_IdAndTypeAndKeyword_Id(Long userId, String type, Long keywordId);
    boolean existsByUser_IdAndKeyword_Id(Long userId, Long keywordId);
    List<UserTypeKeyword> findAllByUser_IdAndKeyword_Id(Long userId, Long keywordId);

    @Query("select distinct utk.user.id from UserTypeKeyword utk " +
            "where utk.type = :type and utk.keyword.id in :keywordIds")
    List<Long> findUserIdsByTypeAndKeywordIds(@Param("type") String type,
                                              @Param("keywordIds") List<Long> keywordIds);
}
