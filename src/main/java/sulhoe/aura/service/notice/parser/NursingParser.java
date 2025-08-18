package sulhoe.aura.service.notice.parser;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import sulhoe.aura.entity.Notice;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("nursingParser")
public class NursingParser implements NoticeParser {
    private static final Pattern NO_PATTERN =
            Pattern.compile("(?i)\\bno\\b\\s*[:=]\\s*['\"]?(\\d+)['\"]?");

    @Override
    public int getStep(){ return 1; }

    @Override
    public String buildPageUrl(String baseUrl, int pageIdx) {
        return baseUrl + (baseUrl.contains("?") ? "&" : "?") + "page=" + (pageIdx + 1);
    }

    @Override
    public Notice parseRow(Element row, boolean isFixed, String baseUrl) {
        Elements cols = row.select("td");
        String number = isFixed ? "공지" : textOr(cols, 0);
        String category = textOr(cols, 1);
        Element a = cols.size() > 2 ? cols.get(2).selectFirst("a") : null;
        if (a == null) throw new IllegalArgumentException("Nursing: missing <a> element");

        String title = a.text().trim();

        String href = a.attr("href");
        Matcher m = NO_PATTERN.matcher(href);
        if (!m.find()) throw new IllegalArgumentException("Nursing: cannot extract article no from href: " + href);
        String articleNo = m.group(1);

        // ★ 핵심 수정: List → View, 파라미터는 no=
        String viewBase = baseUrl.contains("List") ? baseUrl.replace("List", "View") : baseUrl;
        String link = viewBase + (viewBase.contains("?") ? "&" : "?") + "no=" + articleNo;

        String dept = "none";
        String date = textOr(cols, 3, "");
        return new Notice(number, category, title, dept, date, link);
    }

    private static String textOr(Elements tds, int idx) { return textOr(tds, idx, ""); }
    private static String textOr(Elements tds, int idx, String def) { return tds.size() > idx ? tds.get(idx).text() : def; }
}
