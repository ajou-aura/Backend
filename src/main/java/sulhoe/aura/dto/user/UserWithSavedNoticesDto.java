package sulhoe.aura.dto.user;

import sulhoe.aura.dto.notice.NoticeDto;

import java.util.List;

public record UserWithSavedNoticesDto(Long id,
                                      String email,
                                      String department,
                                      List<NoticeDto> savedNotices) {
}
