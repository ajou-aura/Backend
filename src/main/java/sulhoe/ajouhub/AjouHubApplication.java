package sulhoe.ajouhub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "sulhoe.ajouhub.repository")
@EntityScan(basePackages = "sulhoe.ajouhub.entity")
@EnableScheduling
public class AjouHubApplication {

    public static void main(String[] args) {
        SpringApplication.run(AjouHubApplication.class, args);
    }

}
