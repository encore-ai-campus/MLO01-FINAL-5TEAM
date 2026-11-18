# Nextcloud 기반 팀 공용 파일 저장소

## 1. 개요

### 1.1 목적

팀원 간 프로젝트 참고 자료 및 테스트 파일을 공유하기 위해 Nextcloud 기반 공용 파일 저장소를 구축한다.

Windows 10의 WSL2 환경에서 Docker Compose를 사용하여 Nextcloud와 MariaDB를 독립적인 컨테이너로 구성하고, Cloudflare Quick Tunnel을 통해 외부 HTTPS 접속을 제공한다.

팀원별 계정 및 폴더 접근 권한을 적용하여 파일 공유 범위를 관리한다.

### 1.2 적용 범위

- Windows 10 및 WSL2 기반 서버 환경
- Docker Compose 기반 서비스 구성
- Nextcloud(Apache) 및 MariaDB 컨테이너 분리
- Docker Named Volume을 통한 데이터 저장
- Docker 내부 네트워크를 통한 서비스 간 통신
- Cloudflare Quick Tunnel 외부 접속
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
| External Access | Cloudflare Quick Tunnel |
| External Protocol | HTTPS |
| Internal Protocol | HTTP / Docker 내부 TCP |
| User Authentication | Nextcloud 계정 및 비밀번호 |
| Management | 인프라 담당 |

WSL2 Ubuntu에서 `docker compose`로 Nextcloud 웹, MariaDB, Cron을 각각의 컨테이너에서 실행한다. 웹과 Cron은 같은 Nextcloud 이미지 버전을 사용한다.

MariaDB 11.8은 Nextcloud 34의 권장 데이터베이스 버전이다. `mariadb:11.8.10` 공식 이미지 태그는 2026-11-18 조회에서 확인되지 않았다. 태그가 게시되면 이미지 다운로드와 실제 기동을 검증한다.

### 2.2 전체 아키텍처

```text
                  Team Members
                       │
                       │ HTTPS (443)
                       ▼
              Cloudflare Quick Tunnel
                       │
                 Encrypted Tunnel
                       │
                       ▼
               cloudflared (WSL2)
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

외부 HTTPS는 Cloudflare가 처리하며, WSL2의 `cloudflared`에서 Docker 로컬 포트로 요청을 전달한다.

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

외부 접속과 관련된 기존 포트는 유지하고, 내부 서비스 포트에 `+5` 규칙을 적용한다.

| 서비스 | 기본 포트 | 적용 포트 | 접근 범위 |
|---|---:|---:|---|
| Cloudflare HTTPS | 443 | 443 | 외부 HTTPS |
| Nextcloud Host | 8080 | 8080 | WSL 로컬 |
| Nextcloud Apache | 80 | 80 | 컨테이너 내부 |
| MariaDB | 3306 | **3311** | Docker 내부 |
| Redis (추후 도입 시) | 6379 | **6384** | Docker 내부 |

### 4.2 포트 관리 원칙

- 외부 HTTPS 및 Nextcloud 로컬 접속 포트는 변경하지 않는다.
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

`127.0.0.1:8080` 또는 `localhost:8080`에서 초기 접속을 확인한다. 외부 접속에 사용할 주소는 터널을 설정할 때 신뢰 도메인에 추가한다.

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

## 8. Cloudflare Quick Tunnel

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

팀원 4명에 대한 개별 계정을 생성한다.

| 사용자 | 기본 접근 범위 |
|---|---|
| 관리자 | 계정·폴더·서비스 설정 |
| 팀원 1 | 허용된 폴더 |
| 팀원 2 | 허용된 폴더 |
| 팀원 3 | 허용된 폴더 |
| 팀원 4 | 허용된 폴더 |

관리자 계정은 일반 사용자 계정과 분리한다.

### 9.2 폴더 접근 제어

```text
Nextcloud
├── Team Member 1 → 허용 폴더
├── Team Member 2 → 허용 폴더
├── Team Member 3 → 허용 폴더
├── Team Member 4 → 허용 폴더
└── Shared Folder → 공용 접근
```

- 사용자별 읽기·쓰기·수정·삭제 권한 관리
- 필요한 폴더에만 권한 부여
- 불필요한 공개 공유 링크 제한
- 사용자 인증은 Nextcloud에서 처리
- 관리자 권한과 일반 사용자 권한 분리

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
| 외부 접속 불가 | Quick Tunnel 상태 확인 |
| 웹 서버 오류 | Nextcloud 컨테이너 상태 확인 |
| DB 연결 오류 | MariaDB 상태 및 `3311` 포트 확인 |
| 파일 업로드 실패 | Volume 권한 및 사용량 확인 |
| 터널 주소 변경 | Nextcloud 도메인 설정 갱신 |
| WSL 종료 | WSL·Docker·cloudflared 실행 상태 확인 |
| 컨테이너 재생성 | Named Volume 연결 확인 |

프로젝트 원본 데이터와 중요 산출물은 GitHub 등 별도 저장소에서 관리한다.

---

## 14. 담당 범위

### 14.1 인프라 담당

- Docker Engine 및 Docker Compose 동작 확인
- Nextcloud·MariaDB 컨테이너 관리
- Docker 네트워크 및 내부 포트 관리
- Docker Named Volume 구성
- Cloudflare Quick Tunnel 접속 설정
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
| 사용자별 폴더 | 팀원 요구사항에 따라 구성 |
| 파일 업로드 제한 | 테스트 파일 크기에 따라 결정 |
| 파일 보관 기간 | 임시 파일 정리 기준 |
| 추가 서비스 | Redis 등 도입 여부 검토 |

---

## 16. 참고 사항

- Windows 10의 WSL2 환경에서 실행한다.
- Docker Compose로 컨테이너를 관리한다.
- Caddy는 사용하지 않는다.
- Nextcloud 공식 Apache 이미지를 사용한다.
- 외부 HTTPS 및 로컬 Nextcloud 포트는 기존 설정을 유지한다.
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
