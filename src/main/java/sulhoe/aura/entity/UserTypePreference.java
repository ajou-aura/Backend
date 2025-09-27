package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.*;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor @Builder
@Entity
@Table(name = "user_type_prefs",
        uniqueConstraints = @UniqueConstraint(name="uq_user_type", columnNames = {"user_id","type"}),
        indexes = {
                @Index(name="idx_utp_user", columnList="user_id"),
                @Index(name="idx_utp_type", columnList="type"),
                @Index(name="idx_utp_mode", columnList="mode")
        })
public class UserTypePreference {

    public enum Mode { ALL, KEYWORD, NONE }

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="user_id")
    private User user;

    @Column(nullable=false, length=80)
    private String type;

    @Enumerated(EnumType.STRING)
    @Column(nullable=false, length=10)
    private Mode mode;
}
