package sulhoe.aura.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@Setter
@Getter
@NoArgsConstructor
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

    @ManyToMany(mappedBy = "savedNotices")
    private Set<User> savedByUsers = new HashSet<>();

    public Notice(String number, String category, String title, String department, String date, String link) {
        this.number = number;
        this.category = category;
        this.title = title;
        this.department = department;
        this.date = date;
        this.link = link;
    }
}
