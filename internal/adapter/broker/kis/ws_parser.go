package kis

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"Custom-HTS/internal/core/domain"
	"strconv"
	"strings"
	"time"
)

// parseRawMessage — 파이프(|) 구분으로 1차 분리.
//
// KIS WebSocket 수신 형식:
//
//	"0|H0STCNT0|001|005930^72000^100^..."
//	 ↑ 암호화   ↑ TR_ID  ↑ 건수  ↑ Body
//
// JSON 응답(구독 확인, 에러 등)은 파이프가 없으므로 에러 반환.
// 호출부(handlePipeMessage)는 strings.Contains(msg, "|") 확인 후 호출.
func parseRawMessage(data string) (*pipeMsg, error) {
	parts := strings.SplitN(data, string(pipeSeparator), 4)
	if len(parts) < 4 {
		return nil, fmt.Errorf("파이프 구분 필드 부족: %d (최소 4)", len(parts))
	}

	count, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("건수 파싱 실패: %w", err)
	}

	return &pipeMsg{
		Encrypted: parts[0] == encryptedFlag,
		TrID:      parts[1],
		Count:     count,
		Body:      parts[3],
	}, nil
}

// decryptAES256CBC — KIS 체결통보(H0STCNI0) AES-256-CBC 복호화.
//
// KIS 암호화 규격:
//   - key: WebSocket 최초 연결 시 서버가 전달한 암호화키 (32바이트 문자열)
//   - iv:  암호화키 앞 16바이트
//   - 입력: Base64 인코딩된 암호문
//
// encKey/encIV는 kisWs.OnMessage()에서 wsRawMessage.Body.Output으로 수신 후 저장됨.
func decryptAES256CBC(encrypted, key, iv string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("base64 디코딩 실패: %w", err)
	}

	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", fmt.Errorf("AES 키 생성 실패: %w", err)
	}

	if len(ciphertext) < aes.BlockSize {
		return "", fmt.Errorf("암호문이 너무 짧음: %d bytes", len(ciphertext))
	}
	if len(ciphertext)%aes.BlockSize != 0 {
		return "", fmt.Errorf("암호문 길이가 블록 크기의 배수가 아님: %d", len(ciphertext))
	}

	mode := cipher.NewCBCDecrypter(block, []byte(iv))
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	plaintext, err = pkcs7Unpad(plaintext)
	if err != nil {
		return "", fmt.Errorf("PKCS7 언패딩 실패: %w", err)
	}

	return string(plaintext), nil
}

func pkcs7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("빈 데이터")
	}
	padLen := int(data[len(data)-1])
	if padLen == 0 || padLen > aes.BlockSize || padLen > len(data) {
		return nil, fmt.Errorf("잘못된 패딩 길이: %d", padLen)
	}
	for i := len(data) - padLen; i < len(data); i++ {
		if data[i] != byte(padLen) {
			return nil, fmt.Errorf("잘못된 패딩 바이트")
		}
	}
	return data[:len(data)-padLen], nil
}

// parseStockTrade — H0STCNT0 (실시간 체결) 캐럿(^) 분리 파싱.
//
// pipeMsg.Count 만큼 반복 레코드가 올 수 있어 슬라이스 반환.
// (실제로 KIS 체결 메시지는 건수=1이 대부분이나 규격상 다건 허용)
//
// KIS 필드 순서 (약 40개 중 핵심만 추출):
//
//	[0] 종목코드  [1] 체결시간  [2] 현재가    [3] 전일대비구분
//	[4] 전일대비  [5] 등락률    [7] 시가      [8] 고가
//	[9] 저가      [10] 매도호가1 [11] 매수호가1 [12] 체결수량
//	[13] 누적거래량
//
// fields[3] "4"=하락, "5"=하한 → Change/ChangeRate 음수 처리.
func parseStockTrade(rawData string) ([]*stockTradeMsg, error) {
	fields := strings.Split(rawData, string(fieldSeparator))
	if len(fields) < 14 {
		return nil, fmt.Errorf("체결 데이터 필드 부족: %d (최소 14)", len(fields))
	}

	msg := &stockTradeMsg{
		Code:     fields[0],
		ExecTime: fields[1],
	}

	var err error
	if msg.Price, err = parseKRWInt(fields[2]); err != nil {
		return nil, fmt.Errorf("현재가 파싱: %w", err)
	}
	if msg.Change, err = parseKRWInt(fields[4]); err != nil {
		return nil, fmt.Errorf("전일대비 파싱: %w", err)
	}
	if msg.ChangeRate, err = parseRate(fields[5]); err != nil {
		return nil, fmt.Errorf("등락률 파싱: %w", err)
	}

	// 하락/하한 구분: 부호 반전
	if fields[3] == "4" || fields[3] == "5" {
		msg.Change = -msg.Change
		msg.ChangeRate = -msg.ChangeRate
	}

	if msg.Open, err = parseKRWInt(fields[7]); err != nil {
		return nil, fmt.Errorf("시가 파싱: %w", err)
	}
	if msg.High, err = parseKRWInt(fields[8]); err != nil {
		return nil, fmt.Errorf("고가 파싱: %w", err)
	}
	if msg.Low, err = parseKRWInt(fields[9]); err != nil {
		return nil, fmt.Errorf("저가 파싱: %w", err)
	}
	if msg.AskPrice, err = parseKRWInt(fields[10]); err != nil {
		return nil, fmt.Errorf("매도호가 파싱: %w", err)
	}
	if msg.BidPrice, err = parseKRWInt(fields[11]); err != nil {
		return nil, fmt.Errorf("매수호가 파싱: %w", err)
	}
	if msg.Volume, err = parseInt64(fields[12]); err != nil {
		return nil, fmt.Errorf("체결수량 파싱: %w", err)
	}
	if msg.AccVolume, err = parseInt64(fields[13]); err != nil {
		return nil, fmt.Errorf("누적거래량 파싱: %w", err)
	}

	return []*stockTradeMsg{msg}, nil
}

// parseStockQuote — H0STASP0 (실시간 호가) 캐럿(^) 분리 파싱.
//
// KIS 호가 필드 순서:
//
//	[0]    종목코드  [1]    호가시간
//	[3~12]  매도호가 1~10    [13~22] 매수호가 1~10
//	[23~32] 매도잔량 1~10    [33~42] 매수잔량 1~10
//	[43] 총매도잔량  [44] 총매수잔량
//
// [2]는 미사용 필드(임시코드)이므로 건너뜀.
func parseStockQuote(rawData string) (*stockQuoteMsg, error) {
	fields := strings.Split(rawData, string(fieldSeparator))
	if len(fields) < 45 {
		return nil, fmt.Errorf("호가 데이터 필드 부족: %d (최소 45)", len(fields))
	}

	msg := &stockQuoteMsg{
		Code:     fields[0],
		ExecTime: fields[1],
	}

	var err error

	for i := 0; i < 10; i++ {
		if msg.Asks[i].Price, err = parseKRWInt(fields[3+i]); err != nil {
			return nil, fmt.Errorf("매도호가[%d] 파싱: %w", i+1, err)
		}
	}
	for i := 0; i < 10; i++ {
		if msg.Bids[i].Price, err = parseKRWInt(fields[13+i]); err != nil {
			return nil, fmt.Errorf("매수호가[%d] 파싱: %w", i+1, err)
		}
	}
	for i := 0; i < 10; i++ {
		if msg.Asks[i].Size, err = parseInt64(fields[23+i]); err != nil {
			return nil, fmt.Errorf("매도잔량[%d] 파싱: %w", i+1, err)
		}
	}
	for i := 0; i < 10; i++ {
		if msg.Bids[i].Size, err = parseInt64(fields[33+i]); err != nil {
			return nil, fmt.Errorf("매수잔량[%d] 파싱: %w", i+1, err)
		}
	}

	if msg.TotalAsk, err = parseInt64(fields[43]); err != nil {
		return nil, fmt.Errorf("총매도잔량 파싱: %w", err)
	}
	if msg.TotalBid, err = parseInt64(fields[44]); err != nil {
		return nil, fmt.Errorf("총매수잔량 파싱: %w", err)
	}

	return msg, nil
}

// parseStockExecNotify — H0STCNI0 (체결통보) 캐럿(^) 분리 파싱.
//
// 호출 전 decryptAES256CBC()로 복호화 필수.
//
// KIS 체결통보 필드 순서:
//
//	[0] 유의코드  [1] 계좌번호  [2] 주문번호
//	[3] 종목코드  [4] 매매구분  [5] 주문구분
//	[6] 주문상태  [7] 체결단가  [8] 체결수량
//	[9] 주문총수량 [10] 미체결수량 [11] 체결시간
//
// 매매구분: "01"=매도, "02"=매수
// 주문상태: "01"=접수, "02"=체결, "03"=부분체결, "04"=거부
func parseStockExecNotify(rawData string) (*stockExecNotify, error) {
	fields := strings.Split(rawData, string(fieldSeparator))
	if len(fields) < 12 {
		return nil, fmt.Errorf("체결통보 필드 부족: %d (최소 12)", len(fields))
	}

	time, err := parseTime(fields[11])
	if err != nil {
		return nil, fmt.Errorf("체결시간 파싱: %w", err)
	}

	side, err := parseSide(fields[4])
	if err != nil {
		return nil, fmt.Errorf("매매구분 파싱: %w", err)
	}

	msg := &stockExecNotify{
		AccountNo: fields[1],
		OrderNo:   fields[2],
		Code:      fields[3],
		Side:      side,
		ExecTime:  time,
	}

	switch fields[6] {
	case "01":
		msg.Status = "accepted"
	case "02":
		msg.Status = "filled"
	case "03":
		msg.Status = "partial"
	case "04":
		msg.Status = "rejected"
	default:
		msg.Status = fields[6]
	}

	var interr error
	if msg.Price, interr = parseKRWInt(fields[7]); interr != nil {
		return nil, fmt.Errorf("체결단가 파싱: %w", interr)
	}
	if msg.Quantity, interr = parseInt64(fields[8]); interr != nil {
		return nil, fmt.Errorf("체결수량 파싱: %w", interr)
	}
	if msg.TotalQty, interr = parseInt64(fields[9]); interr != nil {
		return nil, fmt.Errorf("주문총수량 파싱: %w", interr)
	}
	if msg.RemainQty, interr = parseInt64(fields[10]); interr != nil {
		return nil, fmt.Errorf("미체결수량 파싱: %w", interr)
	}

	return msg, nil
}

func parseTime(raw string) (time.Time, error) {
	// KIS 체결시간은 "HHMMSS" 형식. 24시간제.
	return time.ParseInLocation("150405", raw, time.Local)
}

func parseSide(raw string) (domain.Side, error) {
	switch raw {
	case "01":
		return domain.Sell, nil
	case "02":
		return domain.Buy, nil
	default:
		return "", fmt.Errorf("알 수 없는 매매구분: %s", raw)
	}
}
