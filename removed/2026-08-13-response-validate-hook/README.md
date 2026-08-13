# response-validate-hook (2026-08-13 제거)

본 레포 루트의 `internal/` 스냅샷은 구세대 코드 전체를 통째로 보관한 것이고,
이 디렉터리는 그 이후 **현행 코드에서 개별적으로 떼어낸 조각**을 담는다.
세대가 섞이지 않도록 `removed/<제거일>-<주제>/` 아래에 원본 경로를 그대로 재현한다.

- 원본 레포: [blackpigfoot/Custom-HTS](https://github.com/blackpigfoot/Custom-HTS)
- 제거 커밋: `28b3803` (커스텀 에러 정리와 응답 판정 흐름 단순화)

응답 body에 담긴 벤더 business error를 **전송 계층에서 자동 재시도**하던 장치.
`requester.Item.Validate` 훅과 그것을 쓰던 kis 어댑터의 판정 함수로 이루어져 있었다.

### 왜 있었나

KIS는 토큰 만료(`EGW00123`)와 유량 초과(`EGW00201`)를 HTTP status가 아니라
`200 OK` + 응답 body로 알려준다. 전송 계층만 보는 requester는 이를 판별할 수 없어서,
벤더 지식이 필요한 판별을 어댑터가 콜백으로 주입하고 재시도 여부만
`Retryable` 표시로 되돌려주는 구조였다.

### 왜 뺐나

1. **사용자에게 보이는 실패 경로는 어차피 필요하다.** 네트워크 호출인 이상 UI/CLI에는
   실패 상태와 재요청 수단이 있어야 한다. 자동 재시도는 그 경로를 없애지 못하고
   발생 빈도만 낮추는데, 그 대가로 콜백 기반의 뒤집힌 제어 흐름을 떠안았다.
2. **선제 방어가 이미 둘 다 있다.** 토큰은 `renewable`이 만료 10분 전에 갱신하고
   (`tokenExpiryMargin`), 유량은 `rate.Limiter`가 앞단에서 막는다. 두 msg_cd는
   사실상 백스톱이라 발화 빈도가 낮다.
3. **비멱등 요청에는 오히려 위험하다.** 주문(`order-cash`) 같은 호출의 자동 재시도는
   중복 주문 위험을 안고 간다.
4. 재시도를 버리자 판정이 콜백 밖으로 나왔다. `sendREST`가
   "조립 → 전송 → 판정" 순의 평범한 순차 코드가 되면서 훅·클로저·마커 타입이 모두 불필요해졌다.

### 남긴 것

**복구는 남기고 재시도만 뺐다.** `handleKISError`가 `EGW00123`에서 `auth.ClearToken()`을
계속 호출한다. `cachedToken.IsValid()`가 로컬 만료 시각만 보기 때문에, 폐기하지 않으면
서버가 이미 버린 토큰을 로컬 만료 시각까지 계속 재사용해 일시적 실패가 영구적 실패로 굳는다.
폐기해두면 호출자가 다시 보낼 때 새 토큰이 발급되어 자가 치유된다.

### 언제 되살릴 만한가

사람이 재요청해줄 수 없는 **무인 흐름**(자동매매 스케줄러, 조건 감시)이 생겼을 때.
다만 그때도 전송 계층보다는 전략 계층에서 재시도하는 편이 맞을 가능성이 높다.
전송 계층에서 되살린다면 이 디렉터리의 파일을 원래 자리로 되돌리면 된다.

### 파일

여러 파일에 흩어져 있던 조각이라 단순 이동이 되지 않았다. 원문을 그대로 옮기고
각 조각이 원래 어느 파일 어디에 있었는지 파일 안에 주석으로 표시해 뒀다.
아래 경로는 원본 레포 기준이며, 이 디렉터리에도 같은 경로로 재현되어 있다.

| 파일 | 흩어져 있던 원본 |
| --- | --- |
| `internal/core/pkg/requester/requester_validate_hook.go` | `errors.go`, `types.go`, `requester.go` |
| `internal/core/pkg/requester/requester_validate_hook_test.go` | `errors_test.go`, `requester_test.go` |
| `internal/adapter/broker/kis/kis_classify_response.go` | `rest.go`, `errors.go`, `rest_types.go` |

빌드 대상이 아니다. 원본 레포에 남아 있는 심볼을 참조하므로 이 레포에서는 컴파일되지 않는다.
