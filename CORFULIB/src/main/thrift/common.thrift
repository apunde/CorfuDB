namespace java com.microsoft.corfu


enum CorfuErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
	OK_SKIP
}

enum ExtntMarkType {	EX_EMPTY, EX_FILLED, EX_TRIMMED, EX_SKIP }

struct ExtntInfo {
	1: i64 metaFirstOff,
	2: i32 metaLength,
	3: ExtntMarkType flag=ExtntMarkType.EX_FILLED
}

typedef binary LogPayload

struct ExtntWrap {
	1: CorfuErrorCode err,
	2: ExtntInfo inf,
	3: list<LogPayload> ctnt,
	}
	

