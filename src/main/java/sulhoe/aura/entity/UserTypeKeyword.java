package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.*;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor @Builder
@Entity
@Table(name = "user_type_keywords",
        uniqueConstraints = @UniqueConstraint(name="uq_user_type_keyword", columnNames = {"user_id","type","keyword_id"}),
        indexes = {
                @Index(name="idx_utik_user", columnList="user_id"),
                @Index(name="idx_utik_type", columnList="type"),
                @Index(name="idx_utik_keyword", columnList="keyword_id")
        })
public class UserTypeKeyword {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="user_id")
    private User user;

    @Column(nullable=false, length=80)
    private String type;

    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="keyword_id")
    private Keyword keyword;
}
