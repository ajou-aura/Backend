package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = "users")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String email;

    @Column(nullable = false)
    private String refreshToken;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "user_departments",
            joinColumns = @JoinColumn(name = "user_id")
    )
    @Column(name = "department")
    private Set<String> departments = new HashSet<>();

    @ManyToMany
    @JoinTable(
            name = "user_saved_notices",
            joinColumns = @JoinColumn(name = "user_id"),
            inverseJoinColumns = @JoinColumn(name = "notice_id")
    )
    private Set<Notice> savedNotices = new HashSet<>();

    public User(String name, String email, Set<String> departments) {
        this.name = name;
        this.email = email;
        this.departments = departments;
    }
}
