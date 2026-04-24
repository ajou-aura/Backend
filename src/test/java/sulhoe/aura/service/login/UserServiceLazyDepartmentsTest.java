package sulhoe.aura.service.login;

import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import sulhoe.aura.dto.user.UserResponseDto;
import sulhoe.aura.entity.User;
import sulhoe.aura.repository.UserRepository;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests verifying that User.departments with FetchType.LAZY:
 * 1. Works correctly inside a @Transactional boundary
 * 2. Throws LazyInitializationException when accessed outside a transaction
 * 3. API response shape remains unchanged (departments included)
 */
@SpringBootTest
@ActiveProfiles("test")
class UserServiceLazyDepartmentsTest {

    @Autowired
    private UserService userService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PlatformTransactionManager txManager;

    @MockBean
    private com.google.firebase.FirebaseApp firebaseApp;

    private TransactionTemplate txTemplate;

    private static final String TEST_EMAIL = "lazy-test@ajou.ac.kr";

    @BeforeEach
    void setUp() {
        txTemplate = new TransactionTemplate(txManager);
        userRepository.deleteAll();
        User user = new User("Lazy Test", TEST_EMAIL, Set.of("컴퓨터공학과", "전자공학과"));
        user.setRefreshToken("test-refresh-token");
        userRepository.save(user);
    }

    // ===== Test 1: Departments accessible inside @Transactional =====

    @Test
    void getUserInfoByEmail_returnsDepartmentsInsideTransaction() {
        UserResponseDto dto = userService.getUserInfoByEmail(TEST_EMAIL);

        assertThat(dto.email()).isEqualTo(TEST_EMAIL);
        assertThat(dto.name()).isEqualTo("Lazy Test");
        assertThat(dto.departments()).containsExactlyInAnyOrder("컴퓨터공학과", "전자공학과");
    }

    @Test
    void getDepartmentsByEmail_returnsAllDepartmentsInsideTransaction() {
        Set<String> depts = userService.getDepartmentsByEmail(TEST_EMAIL);

        assertThat(depts).containsExactlyInAnyOrder("컴퓨터공학과", "전자공학과");
    }

    // ===== Test 2: LazyInitializationException outside transaction =====

    @Test
    void accessingDepartmentsOutsideTransaction_throwsLazyInitializationException() {
        User detachedUser = txTemplate.execute(status -> {
            User u = userRepository.findByEmailIgnoreCase(TEST_EMAIL).orElseThrow();
            assertThat(u.getName()).isEqualTo("Lazy Test");
            return u;
        });

        // PersistentSet was never initialized within the transaction
        assertThatThrownBy(() -> detachedUser.getDepartments().size())
                .isInstanceOf(LazyInitializationException.class);
    }

    @Test
    void accessingDepartmentsInsideTransaction_doesNotThrow() {
        Set<String> departments = txTemplate.execute(status -> {
            User u = userRepository.findByEmailIgnoreCase(TEST_EMAIL).orElseThrow();
            Set<String> depts = u.getDepartments();
            assertThat(depts).isNotEmpty();
            return depts;
        });

        assertThat(departments).containsExactlyInAnyOrder("컴퓨터공학과", "전자공학과");
    }

    // ===== Test 3: API response shape unchanged =====

    @Test
    void getUserInfoByEmail_responseShapeIncludesDepartments() {
        UserResponseDto dto = userService.getUserInfoByEmail(TEST_EMAIL);

        assertThat(dto).isNotNull();
        assertThat(dto.name()).isEqualTo("Lazy Test");
        assertThat(dto.email()).isEqualTo(TEST_EMAIL);
        assertThat(dto.departments()).isNotNull();
        assertThat(dto.departments()).hasSize(2);
    }

    @Test
    void addDepartmentByEmail_persistsNewDepartment() {
        userService.addDepartmentByEmail(TEST_EMAIL, "경영학과");

        UserResponseDto dto = userService.getUserInfoByEmail(TEST_EMAIL);
        assertThat(dto.departments()).contains("경영학과");
        assertThat(dto.departments()).hasSize(3);
    }

    @Test
    void removeDepartmentByEmail_removesDepartment() {
        userService.removeDepartmentByEmail(TEST_EMAIL, "컴퓨터공학과");

        UserResponseDto dto = userService.getUserInfoByEmail(TEST_EMAIL);
        assertThat(dto.departments()).doesNotContain("컴퓨터공학과");
        assertThat(dto.departments()).hasSize(1);
    }
}