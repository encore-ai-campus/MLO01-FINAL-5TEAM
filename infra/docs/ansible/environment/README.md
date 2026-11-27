# 개발 환경 표준화 및 설치 자동화

> 초기 VM용 Bash 설치 설계 기록입니다. 아래 스크립트와 버전은 당시 계획이며, 현재 환경별 설치는 공통 Ansible 역할로 구성할 예정입니다. [현행 구성](../../../README.md) · [검증한 버전](../../support/compatibility/README.md)

## 1. 개요

### 1.1 목적

팀원별 개발 환경 차이로 발생하는 버전 충돌과 설치 오류를 방지하기 위해 Ubuntu 기반 개발 환경을 표준화한다.

프로그램별 Bash 설치 스크립트를 모듈화하고, 통합 실행 스크립트를 통해 필요한 프로그램을 선택적으로 설치할 수 있도록 구성한다.

설치 전 프로그램의 설치 여부와 버전을 확인하여 중복 설치를 방지하며, 버전 불일치 시 기존 환경을 변경하지 않고 오류를 반환한다.

### 1.2 적용 범위

- VMware 기반 Ubuntu 개발 환경(Dev)
- 각 팀원의 Ubuntu VM에서 로컬 실행
- Python, Java, Kafka, Spark, PostgreSQL, Redis 설치
- 공통 프로그램 버전 및 포트 관리
- 프로그램 및 Python 패키지의 의존성 관리
- 선택형 설치 및 필수 의존성 자동 처리
- 기존 프로그램과의 버전 충돌 방지

AWS 및 홈서버 운영 환경(Prod)은 별도로 관리하며, 공통 프로그램의 기준 버전은 가능한 한 동일하게 유지한다.

---

## 2. 개발 환경

### 2.1 기본 환경

| 항목 | 기준 |
|---|---|
| Host OS | Windows |
| Virtualization | VMware Workstation |
| Guest OS | Ubuntu 24.04 LTS |
| Architecture | x86-64 (AMD64) |
| vCPU | 6 vCPU |
| Memory | 16GB |
| Disk | 150GB |
| 설치 자동화 | Bash Shell Script |
| 실행 위치 | 각 팀원의 Ubuntu VM |
| 패키지 관리 | APT, pip 및 프로그램별 설치 방식 |
| Python 가상환경 | venv |

### 2.2 프로그램 기준 버전

| 프로그램 | 기준 버전 | 용도 |
|---|---|---|
| Python | 3.12.x | 백엔드 및 데이터 개발 |
| OpenJDK | 21 LTS | Java 실행 환경 |
| Apache Kafka | 4.4.0 | 메시지 스트리밍 |
| Apache Spark | 4.3.0 | 데이터 처리 |
| PostgreSQL | 18.x | 관계형 데이터베이스 |
| Redis | 8.2.x | 캐시 및 메시지 처리 |
| Django | 5.2 LTS | 백엔드 프레임워크 |

위 버전은 프로젝트 목표 버전이며, 세부 패치 버전과 프로그램 간 호환성은 인프라 담당이 확인하여 확정한다.

Python 애플리케이션에 필요한 패키지는 개발 팀원과 협의하여 선정한다.

---

## 3. 디렉터리 구조

```text
infra/
├── docs/
│   └── ansible/
│       └── environment/
│           └── README.md
│
└── srcs/
    └── ansible/
        └── environment/
            ├── install.sh
            │
            ├── modules/
            │   ├── python.sh
            │   ├── java.sh
            │   ├── kafka.sh
            │   ├── spark.sh
            │   ├── postgresql.sh
            │   └── redis.sh
            │
            ├── configs/
            │   ├── versions.conf
            │   ├── ports.conf
            │   └── dependencies.conf
            │
            ├── requirements/
            │
            └── scripts/
                ├── common.sh
                └── verify.sh
```

### 3.1 구성 요소별 역할

| 구성 요소 | 역할 |
|---|---|
| `install.sh` | 설치 옵션 처리 및 모듈 실행 제어 |
| `modules/` | 프로그램별 설치·설정·검증 |
| `versions.conf` | 프로그램 기준 버전 관리 |
| `ports.conf` | 서비스 포트 관리 |
| `dependencies.conf` | 프로그램 간 의존성 관리 |
| `requirements/` | Python 패키지 및 의존성 목록 관리 |
| `common.sh` | 공통 검사·로그·오류 처리 |
| `verify.sh` | 설치 및 실행 환경 검증 |

`requirements/`의 세부 구성은 개발 팀원의 패키지 선정 이후 인프라 담당이 관리한다.

---

## 4. 설치 방법

### 4.1 설치 스크립트 실행

프로젝트 루트에서 다음 경로로 이동한다.

```bash
cd infra/srcs/ansible/environment
```

**특정 프로그램 설치**

```bash
./install.sh --modules python
```

**여러 프로그램 선택 설치**

```bash
./install.sh --modules python java kafka
```

**Kafka 및 Spark 설치**

```bash
./install.sh --modules kafka spark
```

**전체 프로그램 설치**

```bash
./install.sh --all
```

### 4.2 실행 옵션

| 옵션 | 설명 |
|---|---|
| `--modules` | 지정한 프로그램 선택 설치 |
| `--all` | 전체 프로그램 설치 |

`--modules` 뒤에는 하나 이상의 모듈명을 공백으로 구분하여 입력한다.

선택하지 않은 프로그램은 설치하지 않지만, 선택한 프로그램의 필수 의존성은 자동으로 설치할 수 있다.

지원하지 않는 모듈명이나 잘못된 옵션을 입력하면 오류를 반환한다.

---

## 5. 설치 및 버전 관리 정책

### 5.1 기본 설치 규칙

| 현재 상태 | 처리 |
|---|---|
| 프로그램 미설치 | 목표 버전 설치 (`INSTALL`) |
| 목표 버전 설치됨 | 설치 생략 (`SKIP`) |
| 다른 버전 설치됨 | 오류 출력 및 중단 (`ERROR`) |
| 필수 의존성 미설치 | 목표 버전 자동 설치 |
| 의존성 버전 불일치 | 오류 출력 및 중단 |
| 설치 중 오류 발생 | 오류 출력 및 중단 |

기존 버전이 목표 버전과 다르더라도 자동 삭제, 업데이트, 덮어쓰기를 수행하지 않는다.

### 5.2 설치 처리 흐름

```text
install.sh 실행
       │
       ▼
설치 옵션 및 모듈 확인
       │
       ▼
공통 설정 로드
       │
       ▼
프로그램 및 의존성 확인
       │
       ▼
기존 설치 버전 검사
       │
       ├── 미설치 ──────── INSTALL
       │
       ├── 버전 일치 ───── SKIP
       │
       └── 버전 불일치 ─── ERROR / 중단
       │
       ▼
필요한 프로그램 설치
       │
       ▼
버전·설정·실행 상태 확인
```

### 5.3 버전 불일치 처리

버전 불일치 시 현재 버전과 요구 버전을 출력하고 설치를 중단한다.

```text
[CHECK] Java

Current Version : 17
Required Version: 21

[ERROR] Java version mismatch
Installation aborted.
Exit Code: 1
```

Bash에서는 `exit 1` 등 0이 아닌 종료 코드를 사용하여 오류를 반환한다.

---

## 6. 프로그램 의존성 관리

프로그램 및 패키지 의존성 관리는 인프라 담당이 수행한다.

### 6.1 주요 의존성

| 모듈 | 선행 의존성 |
|---|---|
| Python | Ubuntu 시스템 패키지 |
| Java | Ubuntu 시스템 패키지 |
| Kafka | Java |
| Spark | Java, Python (PySpark 사용 시) |
| PostgreSQL | 관련 패키지 저장소 및 시스템 패키지 |
| Redis | 관련 패키지 저장소 및 시스템 패키지 |

### 6.2 의존성 자동 처리

예를 들어 Kafka와 Spark를 선택한 경우:

```bash
./install.sh --modules kafka spark
```

Java 설치 여부와 버전을 먼저 확인한다.

```text
Kafka + Spark 설치 요청
          │
          ▼
      Java 확인
          │
    ┌─────┼──────┐
    │     │      │
  미설치  일치   불일치
    │     │      │
   설치   SKIP   ERROR
    │     │      │
    └──┬──┘      └── 중단
       │
       ▼
   Kafka / Spark
       │
       ▼
     실행 확인
```

### 6.3 의존성 처리 규칙

- 필요한 프로그램이 없으면 목표 버전을 자동 설치한다.
- 올바른 버전이 설치되어 있으면 생략한다.
- 다른 버전이 설치되어 있으면 오류를 반환한다.
- 공통 의존성은 중복 설치하지 않는다.
- 의존성을 먼저 준비한 후 해당 프로그램을 설치한다.
- Python 패키지 간 의존성 충돌도 인프라 담당이 관리한다.

---

## 7. 공통 설정 관리

프로그램별 설치 스크립트에 버전과 포트를 직접 작성하지 않고 공통 설정 파일로 관리한다.

### 7.1 버전 설정

`configs/versions.conf`

```bash
PYTHON_VERSION="3.12"
JAVA_VERSION="21"
KAFKA_VERSION="4.4.0"
SPARK_VERSION="4.3.0"
POSTGRESQL_VERSION="18"
REDIS_VERSION="8.2"
```

위 값은 목표 버전을 표현한 예시이며, 실제 설치 버전은 프로그램별 버전 비교 규칙을 적용한다.

### 7.2 포트 설정

서비스 기본 포트에서 `+5`를 적용하는 것을 개발 환경 기준으로 하며, SSH·HTTP·HTTPS는 기존 포트를 유지한다.

| 서비스 | 기본 포트 | 개발 표준 포트 |
|---|---:|---:|
| SSH | 22 | 22 |
| HTTP | 80 | 80 |
| HTTPS | 443 | 443 |
| Django | 8000 | 8005 |
| Kafka Broker | 9092 | 9097 |
| Kafka Controller | 9093 | 9098 |
| Spark Master | 7077 | 7082 |
| Spark Master UI | 8080 | 8085 |
| Spark Worker UI | 8081 | 8086 |
| Spark Application UI | 4040 | 4045 |
| PostgreSQL | 5432 | 5437 |
| Redis | 6379 | 6384 |

포트 값은 `configs/ports.conf`에서 관리하며, 서비스 설정에도 동일하게 반영한다.

---

## 8. Python 패키지 관리

### 8.1 관리 방식

Python 실행 환경과 프로젝트 Python 패키지를 구분하여 관리한다.

Python 설치는 `modules/python.sh`에서 담당하며, Python 패키지는 `requirements/`에 필요한 목록을 구성하여 관리한다.

```text
requirements/
└── 필요한 패키지 목록 (*.txt)
```

Python 패키지의 선정은 개발 팀원과 협의하며, 선정된 패키지의 설치 환경·버전 호환성·의존성 관리는 인프라 담당이 수행한다.

### 8.2 관리 원칙

- 프로젝트별 Python 가상환경(`venv`) 사용
- 필요한 Python 패키지를 requirements 파일로 관리
- 개발 팀원은 프로젝트에 필요한 패키지를 선정
- 인프라 담당은 패키지 설치 및 의존성 관리
- 패키지 버전 충돌과 호환성 검증은 인프라 담당이 수행
- 필요한 패키지를 역할 및 사용 목적에 따라 분류

**미정 사항:** Python 패키지 목록, requirements 파일 분류 방식 및 자동 설치 연동 범위

---

## 9. 오류 처리 및 재실행

설치 중 오류가 발생하면 원인을 출력하고 작업을 중단한다.

| 오류 상황 | 처리 |
|---|---|
| 기존 프로그램 버전 불일치 | 오류 출력 후 중단 |
| 의존성 버전 불일치 | 오류 출력 후 중단 |
| 설치 파일 다운로드 실패 | 오류 출력 후 중단 |
| 패키지 설치 실패 | 오류 출력 후 중단 |
| 서비스 설정 오류 | 오류 반환 |
| 프로그램 실행 오류 | 오류 반환 |
| 지원하지 않는 모듈 요청 | 오류 출력 후 중단 |

### 9.1 기존 환경 보호

기존 프로그램 및 데이터를 자동 삭제하지 않는다.

설치에 실패한 경우에도 다른 버전의 프로그램을 강제로 제거하거나 변경하지 않는다.

동일한 명령어를 재실행하면 현재 설치 상태를 다시 확인하여 설치 여부를 판단한다.

부분 설치로 남은 파일이나 불완전한 설치 상태는 정상 설치로 간주하지 않으며, 프로그램별 처리 규칙을 적용한다.

### 9.2 종료 코드

| 종료 코드 | 의미 |
|---|---|
| `0` | 요청한 설치 작업 정상 종료 |
| `0` 이외 | 설치 실패 또는 버전·설정 오류 |

오류 발생 시 후속 설치 모듈의 실행을 중단한다.

---

## 10. 신규 프로그램 추가

새로운 프로그램은 `modules/`에 독립적인 Bash 스크립트를 추가하는 방식으로 확장한다.

```text
modules/
├── python.sh
├── java.sh
├── kafka.sh
├── spark.sh
├── postgresql.sh
├── redis.sh
└── nginx.sh
```

신규 모듈 등록 후 동일한 명령어 구조로 실행한다.

```bash
./install.sh --modules nginx
```

신규 모듈도 공통 버전 검사, 의존성 확인, 오류 처리 규칙을 따른다.

공통 기능은 `scripts/common.sh`로 분리하여 중복 코드를 최소화한다.

---

## 11. 담당 범위

### 11.1 인프라 담당

- Ubuntu 개발 환경 표준화
- Python·Java 등 공통 실행 환경 설치
- Kafka·Spark·PostgreSQL·Redis 설치 및 설정
- Bash 기반 설치 자동화 관리
- 공통 버전 및 포트 설정 관리
- **프로그램 및 Python 패키지 의존성 관리**
- **패키지 버전 충돌 및 호환성 검증**
- requirements 파일 설치 환경 관리
- 설치 오류 처리 및 실행 환경 검증

### 11.2 개발 팀원

- 프로젝트에 필요한 Python 패키지 선정
- 패키지 사용 목적 및 기능 요구사항 전달
- 애플리케이션 및 데이터 처리 코드 개발
- 추가 패키지 필요 시 인프라 담당과 협의

---

## 12. 참고 사항

- 개발 환경 설치 자동화는 Bash 스크립트를 사용한다.
- 각 팀원은 자신의 Ubuntu VM에서 스크립트를 직접 실행한다.
- 기존 버전이 목표와 다르면 자동 삭제하거나 업데이트하지 않는다.
- 필수 의존성이 미설치된 경우 목표 버전을 자동 설치한다.
- 프로그램 및 Python 패키지의 의존성 관리는 인프라 담당이 수행한다.
- Python 패키지 목록은 개발 팀원과 협의하여 확정한다.
- AWS 및 홈서버 운영 환경의 Ansible 자동화는 별도로 관리한다.