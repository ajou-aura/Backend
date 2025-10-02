// sulhoe/aura/service/notice/NoticeTypeLabelResolver.java
package sulhoe.aura.service.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import sulhoe.aura.config.NoticeConfig;

import java.util.Locale;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class NoticeTypeLabelResolver {

    private final NoticeConfig cfg;

    public String labelOf(String type) {
        if (type == null || type.isBlank()) return "알 수 없음";

        final String key = type.trim().toLowerCase(Locale.ROOT);
        final Map<String, String> names = cfg.getNames();

        if (names == null || names.isEmpty()) {
            return "알 수 없음";
        }

        // 1) 정확히 일치하는 키 우선
        String v = names.get(key);
        if (v != null && !v.isBlank()) return v;

        // 2) 'department.ece' 같은 계층형 키에 대해 suffix(예: ece)로도 탐색 허용
        int idx = key.lastIndexOf('.');
        if (idx > 0) {
            String suffix = key.substring(idx + 1);
            v = names.get(suffix);
            if (v != null && !v.isBlank()) return v;
        }

        // 3) 매핑 없으면 고정 기본값
        return "알 수 없음";
    }
}
