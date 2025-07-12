package sulhoe.ajouhub.service.notice.parser;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sulhoe.ajouhub.entity.Notice;

public interface NoticeParser {
    default int getStep(){
        return 10;
    };

    default Elements selectFixedRows(Document doc) {
        return doc.select("table tbody tr.b-top-box");
    }

    default Elements selectGeneralRows(Document doc) {
        return doc.select("table tbody tr:not(.b-top-box):not(:has(td[colspan]))");
    }

    // ScrapeService 에서 '다음 페이지' URL 을 만들 때 사용
    default String buildPageUrl(String baseUrl, int pageIdx) {
        return baseUrl + "?mode=list&&articleLimit=10&article.offset=" + pageIdx;
    }

    // 한 행(row) 을 Notice 로 파싱
    Notice parseRow(Element row, boolean isFixed, String baseUrl);
}
