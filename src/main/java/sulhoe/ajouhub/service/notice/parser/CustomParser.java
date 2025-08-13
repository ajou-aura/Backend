package sulhoe.ajouhub.service.notice.parser;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import sulhoe.ajouhub.entity.Notice;

import java.util.Objects;

@Component("customParser")
public class CustomParser implements NoticeParser {
    @Override
    public Notice parseRow(Element row, boolean isFixed, String baseUrl) {
        Elements cols = row.select("td");
        String number = isFixed ? "공지" : cols.get(0).text();
        String category = "none";
        Element a = cols.get(1).selectFirst("a");
        String title = Objects.requireNonNull(a).text();
        String articleNo = a.attr("href").split("articleNo=")[1].split("&")[0];
        String link = baseUrl + (baseUrl.contains("?") ? "&" : "?") + "mode=view&articleNo=" + articleNo;
        String dept = cols.get(3).text();
        String date = cols.get(4).text();
        return new Notice(number, category, title, dept, date, link);
    }
}
