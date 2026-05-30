# Homestock

Homestock은 신한 Indi OCX를 Windows 32-bit 환경에서 실행하고, 시세/계좌/주문/알림 기능을 MCP Streamable HTTP로 노출하는 Python 서버입니다. Indi가 없는 개발 PC에서도 작업할 수 있도록 mock backend를 함께 제공합니다.

이 프로젝트는 신한투자증권과 제휴하거나 보증받지 않은 **비공식(unofficial) third-party 도구**입니다.

이 저장소는 증권 계좌 데이터와 실주문 기능을 다룹니다. 공개 인터넷에 직접 노출하지 말고, 실계좌 환경에서는 환경변수와 네트워크 경계를 반드시 확인한 뒤 실행하세요.

## 주요 기능

- `/mcp` 경로의 MCP Streamable HTTP 서버
- 로컬 개발과 테스트용 mock backend
- `GIEXPERTCONTROL.GiExpertControlCtrl.1` 기반 real Shinhan Indi backend
- 시세, 기술지표, 호가 snapshot, 종목 뉴스, DART 공시 본문, 계좌 요약, 잔고, 체결, 미체결, 매매내역, 계좌 원장 tool
- 실시간 시세, 뉴스, 공시, 가격 알림, 주가 step callback, fall-safe, 시스템 callback 상태 관리
- 실주문 보호 장치: 기본값 `ALLOW_LIVE_ORDERS=false`에서는 주문/정정/취소가 차단됨

## 안전 기본값

- 개발 기본값은 `INDI_BACKEND=mock`입니다.
- 실주문은 `ALLOW_LIVE_ORDERS=true`를 명시한 경우에만 backend까지 전달됩니다.
- 현재 서버에는 애플리케이션 레벨 인증이 없습니다. 기본 운영은 `127.0.0.1` bind, SSH tunnel, HTTPS reverse proxy, 방화벽 등 별도 보호 경계 안에서만 사용하세요.
- 계좌 비밀번호와 API key는 환경변수로만 주입합니다. `.env`, 로그, runtime state, SSH key, 로컬 bot sidecar는 커밋하지 않습니다.
- `bot/` 및 `discord_bot/` 폴더는 이 저장소 공개 범위에서 제외합니다.

## 요구사항

- Python 3.10+
- real backend 실행용 Windows 환경
- `INDI_BACKEND=real` 실행 시 32-bit Python
- 신한 Indi 설치 및 로그인
- OCX 접근용 `PyQt5` + `QAxContainer`

## 빠른 시작: Mock Backend

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"

$env:INDI_BACKEND = "mock"
$env:ALLOW_LIVE_ORDERS = "false"
$env:HOMESTOCK_HOST = "127.0.0.1"
$env:HOMESTOCK_PORT = "8000"

python -m homestock
```

MCP endpoint:

```text
http://127.0.0.1:8000/mcp
```

## Real Backend

Indi가 설치된 Windows 장비에서 32-bit 가상환경을 만들고 real backend 의존성을 설치합니다.

```powershell
py -3.13-32 -m venv .venv_x86
.\.venv_x86\Scripts\python.exe -m pip install -U pip
.\.venv_x86\Scripts\python.exe -m pip install -e ".[real]"
```

서버 실행 전에 진단 스크립트로 32-bit Python, 환경변수, `PyQt5.QAxContainer`, OCX 생성 가능 여부를 확인합니다.

```powershell
$env:INDI_BACKEND = "real"
$env:ALLOW_LIVE_ORDERS = "false"
.\.venv_x86\Scripts\python.exe .\scripts\check_real_env.py
```

저장소 루트의 32-bit 가상환경을 사용하는 기본 실행 스크립트:

```powershell
.\start_homestock_mcp_server.bat
```

루트 `start_homestock_mcp_server.bat`는 `scripts\start_homestock_mcp.cmd`를 호출하는 shim입니다.
정본 시작 스크립트는 저장소 루트를 기준으로 `.venv_x86`, `logs`, `.runtime` 경로를 계산합니다.
운영 시작 스크립트는 `ALLOW_LIVE_ORDERS`가 미설정이면 `false`로 설정합니다. 실주문이 필요하면 환경변수로 명시적으로 `true`를 지정하세요.

## Vendor 파일 (`qry/`)

real backend의 TR 처리는 신한 Indi 설치에 포함된 query/catalog 파일에 기반합니다.
필드 구조가 필요하면 본인 Indi 설치의 아래 파일을 참고하세요.

- `qry/config/helpertrinfo.dat` — TR 입출력 필드 카탈로그
- `qry/data/xBusPCMapper.conf` — 실시간 시세 필드 매퍼

## 환경변수

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `INDI_BACKEND` | `mock` | `mock` 또는 `real` |
| `ALLOW_LIVE_ORDERS` | `false` | 실주문 전달 허용 여부 |
| `HOMESTOCK_HOST` | `0.0.0.0` | HTTP bind host. 보호 경계가 없다면 `127.0.0.1` 권장 |
| `HOMESTOCK_PORT` | `8000` | HTTP port |
| `HOMESTOCK_RUNTIME_STATE_DIR` | 자동 | 구독/알림 runtime state 저장 위치 |
| `HOMESTOCK_ACCOUNT_PASSWORD` | 미설정 | 계좌/잔고/주문 TR에 필요한 계좌 비밀번호 |
| `HOMESTOCK_USE_THREADED_REAL_CLIENT` | `true` | real Indi client를 전용 thread에서 실행 |

## MCP Client 설정

MCP client가 필요하면 로컬 `.mcp.json`을 만들어 localhost를 가리키게 설정합니다.

```json
{
  "mcpServers": {
    "homestock": {
      "type": "http",
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

원격 host로 바꿀 때는 SSH tunnel, VPN, HTTPS reverse proxy, 방화벽 등 보호 수단을 먼저 준비하세요.

## 테스트

```powershell
python -m pip install -e ".[dev]"
pytest
```

단위 테스트는 mock backend를 사용하므로 Indi 설치가 필요하지 않습니다.

## 프로젝트 구조

- `homestock/`: 서버, tool layer, backend 추상화, runtime state, 모델
- `homestock/indi/`: mock, real, threaded Indi client
- `scripts/`: MCP 서버 운영 및 배포 보조 스크립트
- `tests/`: mock backend 기반 단위 테스트

## 면책

이 프로젝트는 투자 조언이 아니며 신한투자증권과 공식 제휴 또는 보증 관계가 없습니다. 증권사 이용 약관, API 사용, 계좌 보안, 주문 실행 결과에 대한 책임은 실행자에게 있습니다.
