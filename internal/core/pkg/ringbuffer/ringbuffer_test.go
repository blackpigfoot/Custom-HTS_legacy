package ringbuffer

import (
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkRingBuffer_MPMC_4P4C는 총 처리개수가 4배임
// 따라서 b.N도 4배로 늘려서 비교해야 공정함

// TestMultiProducerMultiConsumer
// 상황: 생산자 4명 vs 소비자 4명 (Broadcast 패턴)
// 검증: 모든 소비자가 생산된 모든 데이터를 빠짐없이 받았는가?
// 4 Producers -> 4 Consumers 벤치마크
func BenchmarkRingBuffer_MPMC_4P4C(b *testing.B) {
	rb := NewRingBuffer()
	
	// 생산자별 할당량 계산 (정수 나눗셈)
	itemsPerProducer := b.N / 4
	
	// [핵심 수정] 
	// b.N이 아니라 "실제로 생산될 총개수"를 계산해서 타겟으로 잡아야 함
	// 예: b.N=10 -> producer=2 -> total=8. 소비자는 8을 기다려야 함.
	totalProduced := uint64(itemsPerProducer * 4)
	
	// 만약 테스트 횟수가 너무 적어서 0개라면 바로 종료
	if totalProduced == 0 {
		return
	}

	// 소비자 4명 등록
	cursors := make([]*uint64, 4)
	for i := 0; i < 4; i++ {
		cursors[i] = rb.AddConsumer()
	}

	var wg sync.WaitGroup
	wg.Add(4)

	// 소비자 실행
	for i := 0; i < 4; i++ {
		go func(idx int) {
			defer wg.Done()
			cursor := cursors[idx]
			current := *cursor
			
			for {
				next := rb.WaitForBusy(current)
				
				// 읽기 처리 (Zero Copy)
				_ = rb.Get(next)

				current = next
				atomic.StoreUint64(cursor, current)
				
				// [핵심 수정] 종료 조건
				// 시퀀스는 0부터 시작하므로 (총개수 - 1)이 마지막 번호입니다.
				if next >= totalProduced - 1 { 
					return 
				}
			}
		}(i)
	}

	b.ResetTimer()

	// 생산자 실행
	var pWg sync.WaitGroup
	pWg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer pWg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				// MPMC 환경이므로 반드시 Thread-Safe한 Publish를 써야 함
				rb.Publish(1, 10.0)
			}
		}()
	}

	pWg.Wait() // 생산자 종료 대기
	wg.Wait()  // 소비자 종료 대기
}

// 비교용: Channel MPMC
func BenchmarkChannel_MPMC_4P4C(b *testing.B) {
	ch := make(chan MarketTick, RingSize)
	var wg sync.WaitGroup

	// 소비자 4명 (채널은 경쟁적으로 가져감 - Work Queue 패턴)
	// 주의: 링버퍼는 Broadcast(모두 받음)이고 채널은 Work Queue(나눠 받음)이라
	// 1:1 비교가 약간 불공정하지만 처리량(Throughput) 관점에서 비교합니다.
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			for range ch {
			}
		}()
	}

	b.ResetTimer()

	// 생산자 4명
	var pWg sync.WaitGroup
	pWg.Add(4)
	itemsPerProducer := b.N / 4
	for i := 0; i < 4; i++ {
		go func() {
			defer pWg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				ch <- MarketTick{}
			}
		}()
	}

	pWg.Wait()
	close(ch)
	wg.Wait()
}