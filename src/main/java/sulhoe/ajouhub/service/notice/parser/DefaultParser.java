package sulhoe.ajouhub.service.notice.parser;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;
import sulhoe.ajouhub.entity.Notice;

import java.util.Objects;

@Component("defaultParser")
public class DefaultParser implements NoticeParser {
    @Override
    public Notice parseRow(Element row, boolean isFixed, String baseUrl) {
        Elements cols = row.select("td");
        String number = isFixed ? "공지" : cols.get(0).text();
        String category = cols.get(1).text();
        Element a = row.selectFirst("td a");
        String title = Objects.requireNonNull(a).text();
        String articleNo = a.attr("href").split("articleNo=")[1].split("&")[0];
        String link = baseUrl + "?mode=view&articleNo=" + articleNo;
        String dept = cols.get(4).text();
        String date = cols.get(5).text();
        return new Notice(number, category, title, dept, date, link);
    }
}
