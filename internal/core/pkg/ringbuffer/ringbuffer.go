package ringbuffer

import (
	"runtime"
	"sync/atomic"
	"time"
)

// 링버퍼 크기: 반드시 2의 제곱수여야 함 (65536, 1048576 등)
const (
	RingSize = 1024 * 64
	RingMask = RingSize - 1
)

// 실제 전송될 틱 데이터 (Memory Alignment 고려)
type MarketTick struct {
	SymbolID  int32   // 4 bytes
	Volume    int32   // 4 bytes
	Price     float64 // 8 bytes
	Timestamp int64   // 8 bytes

	_padding [8]byte
}

// -----------------------------------------------------------------------------
// RingBuffer
// -----------------------------------------------------------------------------

type RingBuffer struct {
	// [Cache Line Padding]
	// CPU 캐시 라인(64byte)을 독점하여 False Sharing 방지 (Producer용)
	_padding0 [56]byte

	// 생산자 시퀀스 (Atomic Add로 경쟁)
	producerCursor uint64

	_padding1 [56]byte

	// [성능 최적화] 생산자 전용 캐시 변수 (Atomic 아님)
	// 매번 atomic load를 하지 않기 위해 로컬에 저장해두는 느린 소비자의 위치
	gatingSequenceCache uint64

	// 등록된 소비자들의 커서 포인터 리스트
	consumerCursors []*uint64

	// [Sequencing] 각 슬롯의 데이터가 써졌는지 확인하는 깃발
	flags [RingSize]uint64

	// 실제 데이터 저장소
	data [RingSize]MarketTick
}

// 생성자
func NewRingBuffer() *RingBuffer {
	rb := &RingBuffer{
		// 0번 인덱스부터 쓰기 위해 -1로 초기화
		producerCursor: ^uint64(0),
	}
	// 플래그 초기화: 모든 슬롯을 "아직 쓰이지 않음(-1)" 상태로 설정
	for i := range rb.flags {
		rb.flags[i] = ^uint64(0)
	}
	return rb
}

// 소비자 등록 (반드시 시스템 시작 시 호출)
// 반환된 커서 포인터를 워커가 가지고 있어야 함
func (rb *RingBuffer) AddConsumer() *uint64 {
	cursor := new(uint64)
	*cursor = ^uint64(0) // 초기값 -1
	rb.consumerCursors = append(rb.consumerCursors, cursor)
	return cursor
}

// -----------------------------------------------------------------------------
// 생산자 (Producer) 로직
// -----------------------------------------------------------------------------

func (rb *RingBuffer) Publish(symbolID int32, price float64) {
    // 1. 시퀀스 선점 (Atomic Add는 MPMC에서도 안전함)
    seq := atomic.AddUint64(&rb.producerCursor, 1)

    // 2. [수정됨] Gating 로직 (Atomic 사용)
    // 여러 생산자가 동시에 이 캐시 값을 읽으므로 atomic.Load가 필요합니다.
    cachedGating := atomic.LoadUint64(&rb.gatingSequenceCache)

    if seq - cachedGating > RingSize {
        // 공간이 부족해 보임, 최신 소비자 위치 확인
        var minCursor uint64
        for {
            minCursor = ^uint64(0)
            for _, c := range rb.consumerCursors {
                val := atomic.LoadUint64(c)
                if val < minCursor {
                    minCursor = val
                }
            }

            if seq - minCursor <= RingSize {
                // [수정됨] 캐시 업데이트도 Atomic으로 (여러 생산자가 동시에 업데이트 가능)
                atomic.StoreUint64(&rb.gatingSequenceCache, minCursor)
                break
            }
            runtime.Gosched()
        }
    }

    // 3. 데이터 쓰기 (이전과 동일)
    index := seq & RingMask
    rb.data[index] = MarketTick{
        SymbolID:  symbolID,
        Price:     price,
        Timestamp: time.Now().UnixNano(),
    }
    
    // 4. 커밋 (Flag 설정)
    atomic.StoreUint64(&rb.flags[index], seq)
}

// -----------------------------------------------------------------------------
// 소비자 (Consumer) 대기 전략
// -----------------------------------------------------------------------------

// [전략 워커용] Busy Wait
//
// 특징: 반응속도 가장 빠름 (나노초 단위), CPU 코어 1개 점유
func (rb *RingBuffer) WaitForBusy(readerCursor uint64) uint64 {
	next := readerCursor + 1
	index := next & RingMask

	for {
		// 해당 슬롯에 내 순서(next)에 맞는 데이터가 들어왔는지 확인
		if atomic.LoadUint64(&rb.flags[index]) == next {
			return next
		}
		// CPU 과열 방지를 위해 다른 고루틴에 양보
		runtime.Gosched()
	}
}

// [차트/로그 워커용] Lazy Wait (Sleep)
//
// 특징: 반응속도 느림 (최대 1ms), CPU 거의 안 씀 (데스크탑 친화적)
func (rb *RingBuffer) WaitForLazy(readerCursor uint64) uint64 {
	next := readerCursor + 1
	index := next & RingMask

	for {
		if atomic.LoadUint64(&rb.flags[index]) == next {
			return next
		}
		// 데이터 없으면 1ms 잠자기
		time.Sleep(1 * time.Millisecond)
	}
}

// [유틸리티] 데이터 조회 (Zero-Copy)
// 복사 비용을 없애기 위해 포인터 반환
func (rb *RingBuffer) Get(sequence uint64) *MarketTick {
	return &rb.data[sequence&RingMask]
}
