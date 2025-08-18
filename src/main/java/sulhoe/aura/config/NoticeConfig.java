package sulhoe.aura.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Set;

// 스크래핑할 공지사항 링크들을 카테고리별로 매핑
@Configuration
@ConfigurationProperties(prefix = "notice") // application.properties에서 notice. 이하
@Getter
@Setter
public class NoticeConfig {
    // key = 카테고리 이름, value = 스크래핑할 URL
    private Map<String, String> urls;
    // 별도 작성일 조회가 필요한 카테고리
    private Set<String> categoriesRequirePostedDate;
    // key = 카테고리 이름, value = Spring Bean 이름(NoticeParser)
    private Map<String,String> parser;
}