package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@Setter
@Getter
@NoArgsConstructor
@EntityListeners(AuditingEntityListener.class)
@Table(name = "notices")
public class Notice {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String number;

    @Column
    private String category;

    @Column(nullable = false)
    private String title;

    @Column
    private String department;

    @Column
    private String date;

    @Column
    private String link;

    @CreatedDate
    @Column(updatable = false)
    private LocalDateTime createdAt;  // DB 저장 시각

    @LastModifiedDate
    private LocalDateTime updatedAt;  // DB 수정 시각

    @ManyToMany(mappedBy = "savedNotices")
    private Set<User> savedByUsers = new HashSet<>();

    /** 전역 키워드 태깅 캐시 — JPA가 notice_keywords를 자동 생성 */
    @ManyToMany
    @JoinTable(
            name = "notice_keywords",
            joinColumns = @JoinColumn(name = "notice_id"),
            inverseJoinColumns = @JoinColumn(name = "keyword_id")
    )
    private Set<Keyword> keywords = new HashSet<>(); // 전역 키워드만 넣어 캐시로 사용

    public Notice(String number, String category, String title, String department, String date, String link) {
        this.number = number;
        this.category = category;
        this.title = title;
        this.department = department;
        this.date = date;
        this.link = link;
    }
}
