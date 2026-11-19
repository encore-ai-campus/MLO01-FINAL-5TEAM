# Nextcloud 기반 팀 공용 파일 저장소

## 1. 개요

### 1.1 목적

팀원 간 프로젝트 참고 자료 및 테스트 파일을 공유하기 위해 Nextcloud 기반 공용 파일 저장소를 구축한다.

Windows 10의 WSL2에서 Docker Compose로 Nextcloud와 MariaDB를 실행한다. 팀원은 같은 사내망에서 브라우저로 [Nextcloud](http://192.168.0.152:8080)에 로그인하고 **파일 → share**를 연다.

팀원별 계정 및 폴더 접근 권한을 적용하여 파일 공유 범위를 관리한다.

### 1.2 적용 범위

- Windows 10 및 WSL2 기반 서버 환경
- Docker Compose 기반 서비스 구성
- Nextcloud(Apache) 및 MariaDB 컨테이너 분리
- Docker Named Volume을 통한 데이터 저장
- Docker 내부 네트워크를 통한 서비스 간 통신
- 사내망 `192.168.0.0/24`에서 브라우저 접속
- 내부 서비스 포트 `+5` 표준 적용
- 팀원별 인증 및 폴더 접근 권한 관리
- Docker 데몬 및 컨테이너 최소 권한 설정

본 서비스는 **GitHub 등을 대체하는 원본 저장소가 아닌 보조 공유·테스트 저장소**로 사용한다.

---

## 2. 기본 환경

### 2.1 서버 환경

| 항목 | 기준 |
|---|---|
| Host OS | Windows 10 |
| Linux Environment | WSL2 Ubuntu |
| Container Runtime | Docker Engine (설치 완료 전제) |
| Container Management | Docker Compose 플러그인 (설치 완료 전제) |
| Application | Nextcloud 34.0.6 |
| Web Server | Nextcloud 공식 이미지 `nextcloud:34.0.6-apache` |
| Database | MariaDB 11.8.10 LTS 공식 이미지 `mariadb:11.8.10` |
| Storage | Docker Named Volume |
| 팀원 접속 범위 | 사내망 `192.168.0.0/24` |
| 팀원 접속 주소 | `http://192.168.0.152:8080` (HTTP 평문 전송) |
| Internal Protocol | HTTP / Docker 내부 TCP |
| User Authentication | Nextcloud 계정 및 비밀번호 |
| Management | 인프라 담당 |

WSL2 Ubuntu에서 `docker compose`로 Nextcloud 웹, MariaDB, Cron을 각각의 컨테이너에서 실행한다. 웹과 Cron은 같은 Nextcloud 이미지 버전을 사용한다.

MariaDB 11.8은 Nextcloud 34의 권장 데이터베이스 버전이다. 2026-11-18 로컬 서버에서는 목표 태그 `mariadb:11.8.10`을 받을 수 없어 서버의 `.env`에 `MARIADB_IMAGE=mariadb:11.8.9`를 지정했다. 이 구성으로 DB·웹·Cron을 기동했다. 외부 공개 전에는 목표 버전으로 업데이트하고 다시 검증한다.

### 2.2 전체 아키텍처

```text
                  Team Members
                       │
                       │ 사내망 HTTP
                       ▼
              192.168.0.152:8080
                Windows portproxy
                       │
                       │ HTTP
                       ▼
                127.0.0.1:8080
                       │
                       ▼
┌────────────────────────────────────────┐
│             Docker Compose             │
│                                        │
│  ┌──────────────────────────────────┐  │
│  │       Nextcloud + Apache         │  │
│  │                                  │  │
│  │     Apache Internal Port: 80     │  │
│  │     Authentication / File ACL    │  │
│  └────────────────┬─────────────────┘  │
│                   │                    │
│                   │ db:3311            │
│                   ▼                    │
│  ┌──────────────────────────────────┐  │
│  │             MariaDB              │  │
│  │                                  │  │
│  │       Internal Port: 3311        │  │
│  │       External Port: None        │  │
│  └──────────────────────────────────┘  │
│                                        │
│  Docker Named Volumes                  │
│  ├── nextcloud_data                    │
│  └── db_data                           │
│                                        │
│  Nextcloud Cron                        │
└────────────────────────────────────────┘
```

Windows 방화벽은 서버 Wi-Fi에서 `192.168.0.0/24`의 TCP 8080 접속을 허용하고 다른 IPv4 대역을 차단한다. Windows `portproxy`는 `192.168.0.152:8080` 요청을 `127.0.0.1:8080`으로 전달한다.

Nextcloud는 공식 Apache 이미지를 사용하므로 Caddy와 별도 Nginx 컨테이너는 구성하지 않는다.

---

## 3. 프로젝트 디렉터리 구조

```text
infra/
├── docs/
│   └── dev/
│       └── nextcloud/
│           └── README.md
│
└── srcs/
    └── dev/
        └── nextcloud/
            ├── .env.example
            ├── compose.yaml
            │
            ├── configs/
            │
            ├── scripts/
            │
            └── secrets/
```

| 구성 요소 | 역할 |
|---|---|
| `README.md` | 서버 아키텍처 및 관리 방법 |
| [`.env.example`](../../../srcs/dev/nextcloud/.env.example) | 이미지 태그와 초기 관리자 계정명 예제 |
| `compose.yaml` | 컨테이너·볼륨·네트워크 설정 |
| `configs/` | 서비스 관련 설정 |
| `scripts/` | 실행 및 운영 자동화 |
| `secrets/` | 인증 정보 관리 (Git 제외) |

서버 초기 설정 시 `.env.example`을 같은 디렉터리의 `.env`로 복사하고 이미지 태그와 초기 관리자 계정명을 지정한다.
비밀번호는 `secrets/` 아래의 별도 파일에 저장한다. `.env`와 `secrets/`는 [저장소의 `.gitignore`](../../../../.gitignore)로 Git 추적에서 제외한다.

---

## 4. 포트 관리 정책

### 4.1 포트 설정 기준

사내망 접속과 WSL 로컬 웹 접속은 8080을 사용하고, 내부 서비스 포트에는 `+5` 규칙을 적용한다.

| 서비스 | 기본 포트 | 적용 포트 | 접근 범위 |
|---|---:|---:|---|
| 사내망 웹 | 8080 | 8080 | 서버 Wi-Fi |
| Nextcloud Host | 8080 | 8080 | WSL 로컬 |
| Nextcloud Apache | 80 | 80 | 컨테이너 내부 |
| MariaDB | 3306 | **3311** | Docker 내부 |
| Redis (추후 도입 시) | 6379 | **6384** | Docker 내부 |

### 4.2 포트 관리 원칙

- 서버 IP와 로컬 웹 포트를 변경할 때 Windows 포트 전달, 방화벽 범위, Nextcloud 신뢰 도메인을 함께 확인한다.
- DB와 같은 내부 서비스의 기본 포트는 `+5` 규칙을 적용한다.
- 내부 서비스 포트 변경 시 서버 수신 포트와 클라이언트 연결 포트를 동일하게 설정한다.
- Docker 내부 통신에는 고정 IP 대신 Compose 서비스 이름을 사용한다.
- MariaDB 및 내부 서비스의 포트를 Windows·WSL 호스트에 공개하지 않는다.
- 포트 번호 변경을 보안 기능으로 간주하지 않는다.

---

## 5. Docker Compose 구성

### 5.1 서비스 구성

| 서비스 | 역할 | 저장 방식 |
|---|---|---|
| `nextcloud` | 웹 서비스 및 파일 관리 | `nextcloud_data` |
| `db` | MariaDB 데이터베이스 | `db_data` |
| `cron` | Nextcloud 백그라운드 작업 | Nextcloud Volume 공유 |

### 5.2 Compose 구성

[Compose 설정 파일](../../../srcs/dev/nextcloud/compose.yaml)은 `.env`에서 이미지 태그와 초기 관리자 계정명을 읽고, `secrets/`의 파일을 컨테이너에 전달한다.

MariaDB가 연결을 받을 수 있고 InnoDB 초기화가 끝나면 Nextcloud 웹을 시작한다. 웹의 `/status.php`에서 설치 완료와 정상 상태가 확인되면 Cron을 시작한다.

서버에서는 `127.0.0.1:8080`으로 접속한다. 사내망에서 사용하는 `192.168.0.152:8080`도 Nextcloud 신뢰 도메인에 등록했다.

MariaDB는 `--port=3311` 옵션을 통해 실제 수신 포트를 변경한다.

Nextcloud는 `MYSQL_HOST=db:3311`을 사용하여 MariaDB에 연결한다.

DB 포트는 `ports` 항목에 등록하지 않는다.

`depends_on`은 기본적으로 시작 순서를 보장하지만, 서비스의 준비 완료까지 보장하지는 않는다. 필요 시 Healthcheck를 추가한다.

---

## 6. Docker 네트워크

### 6.1 네트워크 구분

| 네트워크 | 연결 서비스 | 목적 |
|---|---|---|
| `frontend` | Nextcloud | 웹 서비스 네트워크 |
| `backend` | Nextcloud, MariaDB, Cron | DB 및 내부 서비스 통신 |

### 6.2 내부 서비스 통신

```text
Nextcloud
    │
    │ TCP db:3311
    ▼
MariaDB
```

Docker Compose는 서비스 이름을 내부 DNS로 제공하므로 컨테이너 IP를 고정할 필요가 없다.

백엔드 네트워크에 `internal: true`를 적용하며, MariaDB는 해당 네트워크에만 연결한다.

Nextcloud가 정상적으로 DB에 접근할 수 있도록 필요한 연결 권한은 유지한다.

### 6.3 외부 포트 바인딩

```yaml
ports:
  - "127.0.0.1:8080:80"
```

Nextcloud 웹 포트만 WSL 로컬에 바인딩한다.

외부에서 공인 IP로 Nextcloud 또는 MariaDB에 직접 접속하도록 별도 포트포워딩을 설정하지 않는다.

---

## 7. Docker Named Volume

### 7.1 데이터 저장

```text
Docker
├── Nextcloud Container
│   └── nextcloud_data
│
├── MariaDB Container
│   └── db_data
│
└── Cron Container
    └── nextcloud_data
```

| Volume | 용도 |
|---|---|
| `nextcloud_data` | Nextcloud 설정 및 사용자 파일 |
| `db_data` | MariaDB 데이터 |

### 7.2 저장소 원칙

- Windows 개인 폴더 마운트 금지
- WSL 전체 시스템 경로 마운트 금지
- Docker Named Volume 사용
- Nextcloud 및 MariaDB 데이터 분리
- 컨테이너 재생성 시 Volume 유지
- `docker compose down -v` 사용 주의

Named Volume은 컨테이너와 독립적으로 유지되지만, 실제 데이터는 Docker 호스트의 저장장치에 존재한다.

본 서비스는 원본 데이터 저장소가 아닌 테스트용 저장소이므로 별도 고가용성 구성은 사용하지 않는다.

---

## 8. Cloudflare Quick Tunnel (외부 접속 설계 참고)

현재 운영 범위는 사내망이다. 아래 절차는 향후 외부 HTTPS 배포를 검토할 때 참조한다.

### 8.1 접속 구조

```text
External User
      │
      │ HTTPS
      ▼
Cloudflare
      │
      │ Encrypted Tunnel
      ▼
cloudflared (WSL2)
      │
      │ HTTP
      ▼
127.0.0.1:8080
      │
      ▼
Nextcloud Apache
```

### 8.2 터널 실행

```bash
cloudflared tunnel --url http://127.0.0.1:8080
```

발급된 `trycloudflare.com` 주소를 통해 팀원이 Nextcloud에 접속한다.

Quick Tunnel은 임시 테스트용이며, 재시작 시 접속 주소가 변경될 수 있다.

### 8.3 Nextcloud 주소 설정

발급된 도메인을 Nextcloud의 `trusted_domains`에 등록한다.

```bash
docker compose exec -u www-data nextcloud \
  php occ config:system:set trusted_domains 1 \
  --value="example.trycloudflare.com"
```

HTTPS 외부 연결을 인식하도록 설정한다.

```bash
docker compose exec -u www-data nextcloud \
  php occ config:system:set overwriteprotocol \
  --value="https"
```

필요 시 `overwrite.cli.url`과 프록시 관련 설정을 실제 접속 환경에 맞춰 구성한다.

터널 주소가 변경되면 기존 설정도 갱신한다.

---

## 9. 사용자 계정 및 폴더 접근 권한

### 9.1 사용자 관리

팀원 계정은 `ml-lee`, `web-oh`, `pipe-lee`, `ml-kim`, `infra-cho` 다섯 개다. 계정·공유 설정은 별도 관리자 계정에서 관리한다.
팀원은 브라우저에서 로그인한 뒤 **파일 → share**에서 직접 업로드·다운로드한다. 데스크톱 자동 동기화는 설정하지 않는다.

### 9.2 폴더 접근 제어

```text
share/
├── ml-lee/
├── web-oh/
├── pipe-lee/
├── ml-kim/
└── infra-cho/
```

관리자가 공용 폴더 `share/` 자체를 다섯 계정에 각각 공유한다. 팀원 누구나 `share/` 바로 아래에 폴더를 만들 수 있고, 새 폴더는 다른 네 명에게도 같은 위치에 보인다. 팀원은 작성자와 관계없이 공용 폴더 안에서 파일을 업로드·다운로드·수정·이동·삭제할 수 있다. 팀원 권한 값은 `15`(읽기·수정·생성·삭제)이며, 공유 대상은 관리자가 지정한다.

2026-11-18에 다섯 계정과 다섯 폴더의 25가지 조합에서 업로드와 수정을 검증했다. 각 파일을 다른 계정으로 다운로드해 내용 일치를 확인했다. 별도의 사내망 PC에서 서버 상태와 로그인 페이지도 확인했다.

2026-11-19에 `share/` 전체를 공용으로 전환했다. 한 팀원이 바로 아래에 만든 새 폴더가 다섯 계정에 모두 보였고, 서로 올린 파일을 수정하고 기존 팀원 폴더로 옮겨 삭제하는 작업을 확인했다.

---

## 10. Docker 보안 정책

### 10.1 데몬 관리

| 항목 | 정책 |
|---|---|
| Docker 관리 권한 | 인프라 담당자만 보유 |
| Docker Socket | 컨테이너 마운트 금지 |
| Docker TCP API | 외부 공개 금지 |
| Docker Group | 불필요한 사용자 등록 금지 |
| Docker 보안 패치 | 사용 가능한 패치 적용 |

### 10.2 컨테이너 최소 권한

| 설정 | 정책 |
|---|---|
| `privileged` | 사용 금지 |
| `no-new-privileges` | 호환성 확인 후 적용 |
| `network_mode: host` | 사용 금지 |
| `pid: host` | 사용 금지 |
| `cap_add` | 불필요한 권한 추가 금지 |
| Host Bind Mount | 데이터 저장 목적으로 사용 금지 |
| DB Port Publishing | 금지 |
| Named Volume | 사용 |

컨테이너별 기본 권한을 유지하되 불필요한 추가 권한을 제공하지 않는다.

Docker 격리는 호스트 파일 접근 위험을 줄이지만, 완전한 보안 격리를 보장하지는 않는다.

### 10.3 인증 정보 관리

인증 정보는 Compose Secrets로 제공한다.

```text
secrets/
├── db_password.txt
├── db_root_password.txt
└── admin_password.txt
```

파일은 서버 로컬에만 저장하고 Git에 포함하지 않는다.

비밀번호 파일에 대한 접근은 필요한 관리자와 컨테이너로 제한한다.

---

## 11. Nextcloud 백그라운드 작업

Nextcloud 유지보수를 위해 Cron 컨테이너를 별도로 실행한다.

Cron은 Nextcloud와 동일한 Named Volume을 사용하며, 정기적인 작업을 수행한다.

Nextcloud의 백그라운드 작업 방식을 Cron으로 설정한다.

---

## 12. 실행 및 관리

### 12.1 작업 경로

```bash
cd infra/srcs/dev/nextcloud
```

### 12.2 구성 검사

```bash
docker compose config
```

### 12.3 컨테이너 실행

```bash
docker compose up -d
```

### 12.4 상태 확인

```bash
docker compose ps
```

### 12.5 로그 확인

```bash
docker compose logs -f
```

### 12.6 MariaDB 통신 확인

```bash
docker compose exec db \
  mariadb-admin ping -h 127.0.0.1 -P 3311 -u root -p
```

실행 시 비밀번호를 입력한다.

### 12.7 MariaDB 수신 포트 확인

```bash
docker compose exec db \
  mariadb -u root -p -e "SHOW VARIABLES LIKE 'port';"
```

출력된 포트가 `3311`인지 확인한다.

### 12.8 컨테이너 종료

```bash
docker compose down
```

Named Volume을 유지하므로 일반 종료 시 데이터가 삭제되지 않는다.

---

## 13. 운영 및 장애 대응

본 서비스는 팀 프로젝트에서 사용하는 임시 공유·테스트 환경으로 운영한다.

고가용성, 별도 로드밸런서 및 자동 장애 전환은 구성하지 않는다.

| 장애 상황 | 대응 |
|---|---|
| 사내망 접속 불가 | 서버 IP와 TCP 8080 리스너, Windows 포트 전달, 방화벽 규칙 `Nextcloud-Office-8080-AllowOffice` 및 `Nextcloud-Office-8080-BlockOther` 확인 |
| 웹 서버 오류 | Nextcloud 컨테이너 상태 확인 |
| DB 연결 오류 | MariaDB 상태 및 `3311` 포트 확인 |
| 파일 업로드 실패 | Volume 권한 및 사용량 확인 |
| 사내망 주소 변경 | Windows 포트 전달과 방화벽, Nextcloud 신뢰 도메인 갱신 |
| WSL 종료 | WSL 및 Docker 서비스 실행 상태 확인 |
| 컨테이너 재생성 | Named Volume 연결 확인 |

프로젝트 원본 데이터와 중요 산출물은 GitHub 등 별도 저장소에서 관리한다.

---

## 14. 담당 범위

### 14.1 인프라 담당

- Docker Engine 및 Docker Compose 동작 확인
- Nextcloud·MariaDB 컨테이너 관리
- Docker 네트워크 및 내부 포트 관리
- Docker Named Volume 구성
- 사내망 포트 전달·방화벽·신뢰 도메인 관리
- 사용자 및 폴더 권한 관리
- 컨테이너·데몬 권한 관리
- 인증 정보 및 서버 운영 관리

### 14.2 개발 팀원

- 발급된 계정으로 Nextcloud 접속
- 허용된 폴더에 파일 업로드·다운로드
- 지정된 범위 내 파일 수정
- 테스트 및 참고 자료 공유
- 접속 문제 발생 시 인프라 담당자에게 전달

---

## 15. 미정 사항

| 항목 | 내용 |
|---|---|
| 파일 업로드 제한 | 테스트 파일 크기에 따라 결정 |
| 파일 보관 기간 | 임시 파일 정리 기준 |
| 추가 서비스 | Redis 등 도입 여부 검토 |

---

## 16. 참고 사항

- Windows 10의 WSL2 환경에서 실행한다.
- Docker Compose로 컨테이너를 관리한다.
- Caddy는 사용하지 않는다.
- Nextcloud 공식 Apache 이미지를 사용한다.
- 사내망 브라우저 접속과 서버 로컬 Nextcloud 웹 접속은 TCP 8080을 사용한다.
- MariaDB 내부 포트는 기본값 `3306`에서 `3311`로 변경한다.
- Redis 도입 시 내부 포트는 `6384`를 기준으로 검토한다.
- 컨테이너 간 통신에는 Compose 서비스 이름을 사용한다.
- DB 포트는 외부에 공개하지 않는다.
- 데이터는 Docker Named Volume으로 저장한다.
- 과도한 컨테이너 권한 및 Docker Socket 접근을 허용하지 않는다.
- 계정별 파일 권한은 Nextcloud에서 관리한다.
- 본 서비스는 원본 저장소가 아닌 보조 공유·테스트 용도로 사용한다.

## 17. 참고 문서

- [Nextcloud Docker](https://github.com/nextcloud/docker)
- [Nextcloud 34 시스템 요구사항](https://github.com/nextcloud/documentation/blob/stable34/admin_manual/installation/system_requirements.rst)
- [MariaDB 공식 이미지 태그](https://hub.docker.com/_/mariadb/tags)
- [Nextcloud Database Configuration](https://docs.nextcloud.com/server/stable/admin_manual/configuration_database/linux_database_configuration.html)
- [Docker Compose Networking](https://docs.docker.com/compose/how-tos/networking/)
- [Docker Port Publishing](https://docs.docker.com/get-started/docker-concepts/running-containers/publishing-ports/)
- [Cloudflare Quick Tunnel](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/)
