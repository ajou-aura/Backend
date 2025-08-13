package sulhoe.ajouhub.service.notice.parser;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import sulhoe.ajouhub.entity.Notice;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("medicineParser")
public class MedicineParser implements NoticeParser {
    // no: 12345 / no=12345 / no : '12345'
    private static final Pattern NO_PATTERN =
            Pattern.compile("(?i)\\bno\\b\\s*[:=]\\s*['\"]?(\\d+)['\"]?");

    @Override
    public int getStep() { return 1; }

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
        if (a == null) throw new IllegalArgumentException("Medicine: missing <a> element");

        String title = Objects.requireNonNull(a).text().trim();

        String href = a.attr("href");
        Matcher m = NO_PATTERN.matcher(href);
        if (!m.find()) throw new IllegalArgumentException("Medicine: cannot extract article no from href: " + href);
        String articleNo = m.group(1);

        String viewBase = baseUrl.contains("NoticeList.do")
                ? baseUrl.replace("NoticeList.do", "NoticeView.do")
                : baseUrl;
        String link = viewBase + (viewBase.contains("?") ? "&" : "?") + "no=" + articleNo;

        String dept = textOr(cols, 4, "none");
        String date = textOr(cols, 5, "");
        return new Notice(number, category, title, dept, date, link);
    }

    private static String textOr(Elements tds, int idx) { return textOr(tds, idx, ""); }
    private static String textOr(Elements tds, int idx, String def) { return tds.size() > idx ? tds.get(idx).text() : def; }
}
