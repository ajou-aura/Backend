package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/** 공지 분류용 "키워드" (전역/개인) */
@Getter @Setter
@NoArgsConstructor @AllArgsConstructor @Builder
@Entity
@Table(name = "keywords",
        indexes = {
                @Index(name = "idx_keywords_scope", columnList = "scope"),
                @Index(name = "idx_keywords_owner", columnList = "owner_id")
        })
public class Keyword {

    public enum Scope { GLOBAL, USER }

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 120)
    private String phrase;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private Scope scope;   // GLOBAL or USER

    /** USER 스코프일 때만 사용(소유자) */
    @Column(name = "owner_id")
    private Long ownerId;

    /** 전역 키워드에서 파생된 개인 키워드라면 참조(옵션) */
    @Column(name = "global_ref_id")
    private Long globalRefId;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @PrePersist
    void onCreate() {
        if (createdAt == null) createdAt = LocalDateTime.now();
    }
}
