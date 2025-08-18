package sulhoe.aura.service.notice.parser;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import sulhoe.aura.entity.Notice;

@Component("softwareParser")
public class SoftwareParser implements NoticeParser {
    // ★ 페이지는 1씩 증가합니다.
    @Override public int getStep() { return 1; }

    // 소프트웨어 게시판은 ?page= 를 사용합니다.
    @Override
    public String buildPageUrl(String baseUrl, int pageIdx) {
        String sep = baseUrl.contains("?") ? "&" : "?";
        return baseUrl + sep + "page=" + (pageIdx + 1);
    }

    // 목록 행만 정확히 집계: VIEW 링크가 있는 실제 게시글 행으로 한정
    @Override
    public Elements selectGeneralRows(Document doc) {
        // td.responsive03 내부에 VIEW 링크가 있는 tr만 선택
        return doc.select("table tr:has(td.responsive03 a[href*='mode=VIEW'])");
    }

    @Override
    public Notice parseRow(Element row, boolean isFixed, String baseUrl) {
        Element a = row.selectFirst("td.responsive03 a[href*='mode=VIEW']");
        if (a == null) throw new IllegalArgumentException("Software: missing VIEW link");

        String title = a.text().trim();
        String href  = a.attr("href"); // 상대경로 예: /bbs/board.php?...&num=1462&...
        String articleNo = extractParam(href, "num");
        if (articleNo == null || articleNo.isEmpty()) {
            throw new IllegalArgumentException("Software: cannot extract num from href: " + href);
        }

        String link = absolutize(baseUrl, href);

        // 번호/작성자/작성일은 반응형 클래스 기준으로 안전 추출
        String number = textOr(row.selectFirst("td.responsive01"));
        String author = textOr(row.selectFirst("td.responsive04"));
        String date   = textOr(row.selectFirst("td.responsive05"));

        return new Notice(
                isFixed ? "공지" : number,
                "none",
                title,
                (author == null || author.isBlank()) ? "none" : author,
                date == null ? "" : date,
                link
        );
    }

    // ── helpers ───────────────────────────────────────────────────────────
    private static String textOr(Element el) { return el == null ? "" : el.text(); }

    private static String extractParam(String url, String key) {
        int i = url.indexOf(key + "=");
        if (i < 0) return null;
        int s = i + key.length() + 1;
        int e = url.indexOf('&', s);
        return e >= 0 ? url.substring(s, e) : url.substring(s);
    }

    private static String absolutize(String baseUrl, String href) {
        try {
            java.net.URI base = new java.net.URI(baseUrl);
            java.net.URI abs  = base.resolve(href); // 상대경로 → 절대경로
            return abs.toString();
        } catch (Exception e) {
            String origin = baseUrl.replaceFirst("(?i)^(https?://[^/]+).*$", "$1");
            if (href.startsWith("/")) return origin + href;
            return origin + "/" + href;
        }
    }
}
