package api

import "time"

type ChartMinuteRequest struct {
	Shcode string // 단축코드.

	Minute int64 // 분봉 단위 (예: 1 = 1분봉). t8453 에서는 ncnt(틱 단위)로 재사용.

	// 조회 건수.
	// 최대: 압축 2000, 비압축 500.
	// REST API 는 압축 미지원이므로 최대 500.
	Qrycnt int64

	Nday string // 조회 영업일수 ("0": 미사용, ">=1": 사용).

	// 시작일자 (YYYYMMDD, 예: "20220101").
	// 기본값 " " (edate 기준으로 qrycnt 만큼 조회).
	// 시작일 지정 시 설정.
	Sdate string

	// 현재 미지원 필드.
	Stime string

	// 종료일자 (YYYYMMDD, 예: "20221231").
	// 최신 데이터 조회 시 "99999999" 또는 "당일" 설정.
	Edate string

	// 현재 미지원 필드.
	Etime string
	// 압축여부 ("Y": 압축, "N": 비압축).
	// REST API 압축 미지원 → "N" 으로 설정.
	Comp_yn string

	// 전체 데이터를 받을 때까지 연속조회를 자동 수행할지 여부.
	Cont bool

	// 페이지당 요청 타임아웃. 부모 context 에서 파생.
	PageTimeout time.Duration

	// 거래소구분코드 (예: "U": 통합, "K": KRX, "N": NXT).
	// 주식 거래소 필터 (선택). 기본 "0" (전체).
	// 주의: 원본 t8412 TR 은 해당 필드 없음. t8452/t8453 에서 사용.
	Exchgubun string
}

// t8412Request 는 t8412 의 원시 요청 바디.
type t8412Request struct {
	In t8412InBlock `json:"t8412InBlock"` // 입력 블록.
}

// t8412InBlock 은 t8412 의 원시 입력 블록.
type t8412InBlock struct {
	Shcode   string `json:"shcode"`   // 단축코드.
	Ncnt     int64  `json:"ncnt"`     // 요청 데이터 단위 수.
	Qrycnt   int64  `json:"qrycnt"`   // 조회 건수.
	Nday     string `json:"nday"`     // 조회 영업일수.
	Sdate    string `json:"sdate"`    // 시작일자 (YYYYMMDD).
	Stime    string `json:"stime"`    // 시작시간 (현재 미지원).
	Edate    string `json:"edate"`    // 종료일자 (YYYYMMDD).
	Etime    string `json:"etime"`    // 종료시간 (현재 미지원).
	Cts_Date string `json:"cts_date"` // 연속일자.
	Cts_Time string `json:"cts_time"` // 연속시간.
	Comp_yn  string `json:"comp_yn"`  // 압축여부.
}

// T8412Response 는 분봉 TR (t8412) 의 원시 LS 응답 DTO.
type T8412Response struct {
	baseResponse
	T8412OutBlock  T8412OutBlock    `json:"t8412OutBlock"`  // 기본 주식차트 정보 블록.
	T8412OutBlock1 []T8412OutBlock1 `json:"t8412OutBlock1"` // 상세 주식차트 데이터 블록.
}

// T8412OutBlock 은 기본 주식차트 정보를 담는 단일 객체.
type T8412OutBlock struct {
	Shcode   string `json:"shcode"`    // 단축코드.
	Jisiga   int64  `json:"jisiga"`    // 전일시가.
	Jihigh   int64  `json:"jihigh"`    // 전일고가.
	Jilow    int64  `json:"jilow"`     // 전일저가.
	Jiclose  int64  `json:"jiclose"`   // 전일종가.
	Jivolume int64  `json:"jivolume"`  // 전일거래량.
	Disiga   int64  `json:"disiga"`    // 당일시가.
	Dihigh   int64  `json:"dihigh"`    // 당일고가.
	Dilow    int64  `json:"dilow"`     // 당일저가.
	Diclose  int64  `json:"diclose"`   // 당일종가.
	Highend  int64  `json:"highend"`   // 상한가.
	Lowend   int64  `json:"lowend"`    // 하한가.
	Cts_Date string `json:"cts_date"`  // 연속일자.
	Cts_Time string `json:"cts_time"`  // 연속시간.
	STime    string `json:"s_time"`    // 장시작시간.
	ETime    string `json:"e_time"`    // 장종료시간.
	Dshmin   string `json:"dshmin"`    // 동시호가 처리시간.
	RecCount int64  `json:"rec_count"` // 레코드 카운트.
}

// T8412OutBlock1 은 분봉 데이터 한 행을 표현.
type T8412OutBlock1 struct {
	Date     string       `json:"date"`      // 날짜.
	Time     string       `json:"time"`      // 시간.
	Open     int64        `json:"open"`      // 시가.
	High     int64        `json:"high"`      // 고가.
	Low      int64        `json:"low"`       // 저가.
	Close    int64        `json:"close"`     // 종가.
	JdiffVol int64        `json:"jdiff_vol"` // 거래량.
	Value    int64        `json:"value"`     // 거래대금.
	Jongchk  int64        `json:"jongchk"`   // 수정구분.
	Rate     DecimalValue `json:"rate"`      // 수정비율 텍스트.
	Sign     string       `json:"sign"`      // 종가 등락구분 부호.
}

// t8452Request 는 t8452 의 원시 요청 바디.
type t8452Request struct {
	In t8452InBlock `json:"t8452InBlock"` // 입력 블록.
}

// t8452InBlock 은 t8452 의 원시 입력 블록.
type t8452InBlock struct {
	Shcode    string `json:"shcode"`    // 단축코드.
	Ncnt      int64  `json:"ncnt"`      // 요청 데이터 단위 수.
	Qrycnt    int64  `json:"qrycnt"`    // 조회 건수.
	Nday      string `json:"nday"`      // 조회 영업일수.
	Sdate     string `json:"sdate"`     // 시작일자 (YYYYMMDD).
	Stime     string `json:"stime"`     // 시작시간 (현재 미지원).
	Edate     string `json:"edate"`     // 종료일자 (YYYYMMDD).
	Etime     string `json:"etime"`     // 종료시간 (현재 미지원).
	Cts_Date  string `json:"cts_date"`  // 연속일자.
	Cts_Time  string `json:"cts_time"`  // 연속시간.
	Comp_yn   string `json:"comp_yn"`   // 압축여부.
	Exchgubun string `json:"exchgubun"` // 거래소구분코드 ("U": 통합, "K": KRX, "N": NXT).
}

// T8452Response 는 통합 분봉 TR (t8452) 의 원시 LS 응답 DTO.
type T8452Response struct {
	baseResponse
	T8452OutBlock  T8452OutBlock    `json:"t8452OutBlock"`  // 기본 주식차트 정보 블록.
	T8452OutBlock1 []T8452OutBlock1 `json:"t8452OutBlock1"` // 상세 주식차트 데이터 블록.
}

// T8452OutBlock 은 t8452 의 기본 주식차트 정보 블록.
type T8452OutBlock struct {
	Shcode        string `json:"shcode"`        // 단축코드.
	Jisiga        int64  `json:"jisiga"`        // 전일시가.
	Jihigh        int64  `json:"jihigh"`        // 전일고가.
	Jilow         int64  `json:"jilow"`         // 전일저가.
	Jiclose       int64  `json:"jiclose"`       // 전일종가.
	Jivolume      int64  `json:"jivolume"`      // 전일거래량.
	Disiga        int64  `json:"disiga"`        // 당일시가.
	Dihigh        int64  `json:"dihigh"`        // 당일고가.
	Dilow         int64  `json:"dilow"`         // 당일저가.
	Diclose       int64  `json:"diclose"`       // 당일종가.
	Highend       int64  `json:"highend"`       // 상한가.
	Lowend        int64  `json:"lowend"`        // 하한가.
	Cts_Date      string `json:"cts_date"`      // 연속일자.
	Cts_Time      string `json:"cts_time"`      // 연속시간.
	S_Time        string `json:"s_time"`        // 장시작시간 (HHMMSS).
	E_Time        string `json:"e_time"`        // 장종료시간 (HHMMSS).
	Dshmin        string `json:"dshmin"`        // 동시호가 처리시간 (MM).
	RecCount      int64  `json:"rec_count"`     // 레코드 카운트.
	Nxt_fm_s_time string `json:"nxt_fm_s_time"` // NXT 프리마켓 장시작시간 (HHMMSS).
	Nxt_fm_e_time string `json:"nxt_fm_e_time"` // NXT 프리마켓 장종료시간 (HHMMSS).
	Nxt_fm_dshmin string `json:"nxt_fm_dshmin"` // NXT 프리마켓 동시호가 처리시간 (MM).
	Nxt_am_s_time string `json:"nxt_am_s_time"` // NXT 애프터마켓 장시작시간 (HHMMSS).
	Nxt_am_e_time string `json:"nxt_am_e_time"` // NXT 애프터마켓 장종료시간 (HHMMSS).
	Nxt_am_dshmin string `json:"nxt_am_dshmin"` // NXT 애프터마켓 동시호가 처리시간 (MM).
}

// T8452OutBlock1 은 통합 분봉 데이터 한 행을 표현.
type T8452OutBlock1 struct {
	Date     string       `json:"date"`      // 날짜.
	Time     string       `json:"time"`      // 시간.
	Open     int64        `json:"open"`      // 시가.
	High     int64        `json:"high"`      // 고가.
	Low      int64        `json:"low"`       // 저가.
	Close    int64        `json:"close"`     // 종가.
	JdiffVol int64        `json:"jdiff_vol"` // 거래량.
	Value    int64        `json:"value"`     // 거래대금.
	Jongchk  int64        `json:"jongchk"`   // 수정구분.
	Rate     DecimalValue `json:"rate"`      // 수정비율.
	Sign     string       `json:"sign"`      // 수정구분 (1:상한 2:상승 3:보합 4:하한 5:하락).
}

type ChartTickRequest struct {
	Shcode string // 단축코드.
	Ncnt   int64  // 틱 단위 (예: 1 = 1틱).
	Qrycnt int64  // 조회 건수 (최대 500).
	Nday   string // 조회 영업일수 ("0": 미사용, ">=1": 사용).
	Sdate  string // 시작일자 (YYYYMMDD, 예: "20220101").
	Stime  string // 현재 미지원 필드.
	Edate  string // 종료일자 (YYYYMMDD, 예: "20221231").
	Etime  string // 현재 미지원 필드.
	// 압축여부 ("Y": 압축, "N": 비압축).
	// REST API 압축 미지원 → "N" 으로 설정.
	Comp_yn string

	Cont bool // 전체 데이터를 받을 때까지 연속조회를 자동 수행할지 여부.

	PageTimeout time.Duration // 페이지당 요청 타임아웃. 부모 context 에서 파생.

	// 거래소구분코드 (예: "U": 통합, "K": KRX, "N": NXT).
	// 주식 거래소 필터 (선택). 기본 "0" (전체).
	// 주의: 원본 t8412 TR 은 해당 필드 없음. t8452/t8453 에서 사용.
	Exchgubun string
}

// t8453Request 는 t8453 (통합 주식차트 틱/N틱) 의 원시 요청 바디.
type t8453Request struct {
	In t8453InBlock `json:"t8453InBlock"` // 입력 블록.
}

// t8453InBlock 은 t8453 의 원시 입력 블록.
type t8453InBlock struct {
	Shcode    string `json:"shcode"`    // 단축코드.
	Ncnt      int64  `json:"ncnt"`      // 단위 (n틱).
	Qrycnt    int64  `json:"qrycnt"`    // 조회 건수 (최대 500).
	Nday      string `json:"nday"`      // 조회 영업일수 (0: 미사용, >=1: 사용).
	Sdate     string `json:"sdate"`     // 시작일자 (YYYYMMDD).
	Stime     string `json:"stime"`     // 시작시간 (현재 미지원).
	Edate     string `json:"edate"`     // 종료일자 (YYYYMMDD).
	Etime     string `json:"etime"`     // 종료시간 (현재 미지원).
	Cts_Date  string `json:"cts_date"`  // 연속일자.
	Cts_Time  string `json:"cts_time"`  // 연속시간.
	Comp_yn   string `json:"comp_yn"`   // 압축여부 (N: 비압축; OPEN API 압축 미제공).
	Exchgubun string `json:"exchgubun"` // 거래소구분코드 (K: KRX, N: NXT, U: 통합).
}

// T8453Response 는 t8453 의 원시 LS 응답 DTO.
type T8453Response struct {
	baseResponse
	T8453OutBlock  T8453OutBlock    `json:"t8453OutBlock"`
	T8453OutBlock1 []T8453OutBlock1 `json:"t8453OutBlock1"`
}

// T8453OutBlock 은 t8453 의 기본 주식차트 정보 블록.
type T8453OutBlock struct {
	Shcode        string `json:"shcode"`        // 단축코드.
	Jisiga        int64  `json:"jisiga"`        // 전일시가.
	Jihigh        int64  `json:"jihigh"`        // 전일고가.
	Jilow         int64  `json:"jilow"`         // 전일저가.
	Jiclose       int64  `json:"jiclose"`       // 전일종가.
	Jivolume      int64  `json:"jivolume"`      // 전일거래량.
	Disiga        int64  `json:"disiga"`        // 당일시가.
	Dihigh        int64  `json:"dihigh"`        // 당일고가.
	Dilow         int64  `json:"dilow"`         // 당일저가.
	Diclose       int64  `json:"diclose"`       // 당일종가.
	Highend       int64  `json:"highend"`       // 상한가.
	Lowend        int64  `json:"lowend"`        // 하한가.
	Cts_Date      string `json:"cts_date"`      // 연속일자.
	Cts_Time      string `json:"cts_time"`      // 연속시간.
	S_Time        string `json:"s_time"`        // 장시작시간 (HHMMSS).
	E_Time        string `json:"e_time"`        // 장종료시간 (HHMMSS).
	Dshmin        string `json:"dshmin"`        // 동시호가 처리시간 (MM).
	RecCount      int64  `json:"rec_count"`     // 레코드 카운트.
	Nxt_fm_s_time string `json:"nxt_fm_s_time"` // NXT 프리마켓 장시작시간 (HHMMSS).
	Nxt_fm_e_time string `json:"nxt_fm_e_time"` // NXT 프리마켓 장종료시간 (HHMMSS).
	Nxt_fm_dshmin string `json:"nxt_fm_dshmin"` // NXT 프리마켓 동시호가 처리시간 (MM).
	Nxt_am_s_time string `json:"nxt_am_s_time"` // NXT 애프터마켓 장시작시간 (HHMMSS).
	Nxt_am_e_time string `json:"nxt_am_e_time"` // NXT 애프터마켓 장종료시간 (HHMMSS).
	Nxt_am_dshmin string `json:"nxt_am_dshmin"` // NXT 애프터마켓 동시호가 처리시간 (MM).
}

// T8453OutBlock1 은 틱 차트 데이터 한 행을 표현.
type T8453OutBlock1 struct {
	Date     string       `json:"date"`      // 날짜.
	Time     string       `json:"time"`      // 시간.
	Open     int64        `json:"open"`      // 시가.
	High     int64        `json:"high"`      // 고가.
	Low      int64        `json:"low"`       // 저가.
	Close    int64        `json:"close"`     // 종가.
	JdiffVol int64        `json:"jdiff_vol"` // 거래량.
	Jongchk  int64        `json:"jongchk"`   // 수정구분.
	Rate     DecimalValue `json:"rate"`      // 수정비율.
	Pricechk int64        `json:"pricechk"`  // 수정주가 반영항목.
}
