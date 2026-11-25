# MLO01-FINAL-5TEAM Infrastructure

## 1. 프로젝트 개요

MLO 1기 5팀 최종 프로젝트의 **개발·테스트 환경 및 운영 인프라 구축을 위한 자동화 코드와 문서**를 관리하는 디렉터리입니다.

VMware 기반 개발 환경과 Nextcloud 협업 환경을 구축하고, Terraform 및 Ansible을 활용하여 AWS와 홈서버 기반 운영 인프라를 구성하는 것을 목표로 합니다.

### 주요 목표

- **VMware**: Ubuntu 24.04 LTS 기반 개발·테스트 환경 구축
- **Environment**: 개발 환경의 프로그램 및 라이브러리 버전 통일
- **Nextcloud**: 팀 공용 파일 공유 및 동기화 환경 구축
- **AWS Terraform**: AWS 인프라 리소스 생성 및 관리 자동화
- **AWS Ansible**: AWS 서버 설정 및 서비스 배포 자동화
- **Home Server**: 자체 물리 서버 기반 운영 환경 구축 및 관리

---

## 2. 프로젝트 디렉터리 구조

```text
MLO01-FINAL-5TEAM/
│
├── .gitignore                      # Git 추적 제외 규칙
│
└── infra/
    ├── README.md                   # 인프라 개요 및 구조 안내
    ├── AGENTS.md                   # AI 코딩 에이전트 작업 규칙
    │
    ├── docs/                       # 구축 및 설정 문서
    │   │
    │   ├── dev/                    # 개발·테스트 환경 문서
    │   │   ├── vmware/
    │   │   │   └── README.md
    │   │   ├── environment/
    │   │   │   └── README.md
    │   │   └── nextcloud/
    │   │       └── README.md
    │   │
    │   └── prod/                   # 운영 환경 문서
    │       ├── aws/
    │       │   ├── terraform/
    │       │   │   └── README.md
    │       │   └── ansible/
    │       │       └── README.md
    │       └── homeserver/
    │           └── README.md
    │
    └── srcs/                       # 실행 코드 및 설정 파일
        │
        ├── dev/                    # 개발·테스트 환경
        │   ├── vmware/
        │   │   └── scripts/
        │   │
        │   ├── environment/
        │   │   ├── ansible/
        │   │   ├── scripts/
        │   │   └── requirements/
        │   │
        │   └── nextcloud/
        │       ├── scripts/
        │       └── configs/
        │
        └── prod/                   # 실제 운영 환경
            ├── aws/
            │   ├── terraform/
            │   └── ansible/
            │
            └── homeserver/
                ├── ansible/
                └── configs/
```

> 위 구조는 기본 설계안이며, 작업 진행에 따라 각 영역 내부에 필요한 파일과 하위 디렉터리를 추가합니다.

---

## 3. 디렉터리별 역할

### 3.1 `dev/` — 개발 및 테스트 환경

팀원들이 동일한 환경에서 개발하고 기능을 검증할 수 있도록 로컬 가상머신과 공통 실행 환경을 구성합니다.

| 디렉터리 | 주요 역할 |
|---|---|
| `vmware/` | Ubuntu 가상머신 구축, 네트워크, SSH, 공유 폴더 및 스냅샷 관리 |
| `environment/` | 개발 도구·라이브러리 설치, 버전 통일 및 실행 환경 자동화 |
| `nextcloud/` | 팀 공용 파일 서버 구축, 사용자 계정 및 동기화 설정 |

**VMware**

- VMware Workstation 및 Ubuntu 24.04 LTS 설치
- CPU·RAM·디스크 자원 할당
- NAT 및 Host-Only 네트워크 구성
- SSH 원격 접속과 공유 폴더 설정
- 기본 환경 검증 및 스냅샷 생성

**Environment**

- Python, Java, Kafka, Spark, PostgreSQL, Redis 등 설치
- Ansible 또는 Bash를 활용한 개발 환경 자동화
- 라이브러리 및 프로그램 버전 관리
- 서비스 포트 및 실행 환경 설정
- 설치 및 실행 결과 검증

**Nextcloud**

- 팀 공용 파일 서버 설치 및 설정
- 사용자 계정과 공유 폴더 관리
- Windows 클라이언트 파일 동기화
- 파일 읽기·쓰기 및 동기화 기능 검증

### 3.2 `prod/` — 운영 환경

AWS와 홈서버를 활용하여 실제 서비스를 실행할 운영 인프라를 구축합니다.

개발 환경과 운영 환경은 설치 자동화 및 설정을 독립적으로 관리하며, 주요 소프트웨어 버전은 동일한 기준을 유지합니다.

| 디렉터리 | 주요 역할 |
|---|---|
| `aws/terraform/` | AWS 인프라 리소스 생성 및 관리 |
| `aws/ansible/` | AWS 서버 설정, 프로그램 설치 및 서비스 배포 |
| `homeserver/ansible/` | 홈서버 프로그램 설치 및 서버 구성 자동화 |
| `homeserver/configs/` | 홈서버 전용 설정 파일 관리 |

**AWS Terraform**

- VPC, Subnet, Route Table 등 네트워크 구성
- EC2 및 Security Group 생성
- 필요한 IAM 및 스토리지 리소스 구성
- Terraform 기반 인프라 재현성 확보

**AWS Ansible**

- Terraform으로 생성한 서버 대상 관리
- 운영용 프로그램 및 라이브러리 설치
- 서버별 역할에 따른 서비스 구성
- 운영 환경 설정 및 배포 자동화

**Home Server**

- 물리 서버 기반 운영 환경 구성
- 운영체제, 네트워크 및 원격 접속 설정
- Ansible을 통한 프로그램 설치 및 서버 구성
- 운영 서비스 실행 및 연결 검증

> 운영 환경의 클러스터 관련 설정은 AWS 또는 홈서버의 해당 디렉터리에서 관리하며, 별도의 `cluster/` 디렉터리는 구성하지 않습니다.

---

## 4. 개발 환경 표준화

### 4.1 운영체제 및 아키텍처

| 항목 | 기준 |
|---|---|
| OS | Ubuntu 24.04 LTS |
| CPU 아키텍처 | x86-64 (AMD64) |
| 가상화 환경 | VMware Workstation |
| 클라우드 환경 | AWS EC2 |
| 운영 환경 | AWS + Home Server |

### 4.2 주요 기술 스택

| 구분 | 기술 | 기준 버전 |
|---|---|---|
| Language | Python | 3.12.x |
| JVM | OpenJDK | 21 LTS |
| Backend | Django | 5.2 LTS |
| Messaging | Apache Kafka | 4.4.0 |
| Data Processing | Apache Spark / PySpark | 4.3.0 |
| Database | PostgreSQL | 18.x |
| Cache | Redis | 8.2.x |
| Configuration Management | Ansible | 추후 확정 |
| Infrastructure as Code | Terraform | 추후 확정 |

> 기준 버전은 설치 및 호환성 검증을 통해 최종 확정하며, Dev와 Prod에서 동일한 버전을 사용하는 것을 원칙으로 합니다.

---

## 5. 프로젝트 관리 원칙

### 5.1 코드 및 문서 관리

- `infra/srcs/`에는 실제 실행 코드와 설정 파일을 관리합니다.
- `infra/docs/`에는 구축 목적, 설치 과정, 설정 방법 및 검증 절차를 작성합니다.
- `infra/docs/`와 `infra/srcs/`는 대응하는 디렉터리 구조를 유지합니다.
- 작업별 상세 README는 `infra/docs/`에서만 관리하여 문서 중복을 방지합니다.
- `infra/README.md`는 인프라 개요 및 전체 구조 안내를 담당합니다.
- 코드 변경으로 설치 방법이나 설정값이 변경될 경우 관련 문서를 함께 갱신합니다.

### 5.2 Git 관리

- Git 저장소는 프로젝트 최상위에서 하나만 관리합니다.
- `.gitignore`는 프로젝트 최상위 파일 하나로 통합 관리합니다.
- 폴더별 제외 규칙은 최상위 `.gitignore`에서 경로를 지정하여 관리합니다.
- 작업별 브랜치를 생성하고 Pull Request를 통해 변경 사항을 병합합니다.
- 민감 정보 및 실행 환경에서 생성되는 불필요한 파일은 Git 추적에서 제외합니다.

### 5.3 개발 및 운영 환경 분리

- Dev와 Prod의 설치 자동화 코드 및 환경 설정은 독립적으로 관리합니다.
- Dev는 단일 VMware 가상머신 기반 개발·테스트 환경으로 구성합니다.
- Prod는 AWS와 홈서버를 활용한 실제 운영 환경으로 구성합니다.
- Prod의 서비스 설치 및 구성은 서버 역할과 운영 아키텍처에 맞게 별도로 관리합니다.
- **Dev와 Prod의 주요 소프트웨어 및 라이브러리 버전은 동일하게 유지합니다.**
- 버전 변경 시 양쪽 환경의 호환성과 설정을 함께 검토합니다.

### 5.4 자동화 및 설정 관리

- 프로그램 설치 및 서버 설정 자동화는 Ansible을 중심으로 구성합니다.
- 최초 환경 준비와 실행 검증 등은 Bash·PowerShell 스크립트로 보완합니다.
- AWS 인프라 리소스 생성은 Terraform으로 관리합니다.
- 환경별 설정값은 변수 및 설정 파일로 분리합니다.
- 자동화 코드는 반복 실행 시 안정적으로 동작하도록 구성하고, 실행 결과를 검증합니다.

### 5.5 보안 및 민감 정보 관리

- 비밀번호, API 키, SSH 개인키 및 인증 정보는 Git에 저장하지 않습니다.
- Terraform State 및 민감한 변수 파일은 별도로 관리합니다.
- 실제 데이터베이스 데이터와 Nextcloud 저장 파일은 코드 저장소에 포함하지 않습니다.
- 설정 파일은 필요한 경우 실제 값 대신 템플릿 또는 예제 파일로 관리합니다.

---

## 6. 구축 진행 단계

| 단계 | 작업 | 주요 목표 |
|---|---|---|
| 1 | VMware | Ubuntu 기반 개발·테스트 환경 구축 |
| 2 | Environment | 라이브러리 및 프로그램 버전 통일 |
| 3 | Nextcloud | 팀 공용 파일 공유 환경 구축 |
| 4 | AWS Terraform | AWS 인프라 자동 생성 |
| 5 | AWS Ansible | AWS 서버 환경 설정 및 서비스 배포 |
| 6 | Home Server | 물리 서버 기반 운영 환경 구축 |
| 7 | 운영 연동 검증 | AWS 및 홈서버 서비스 연결 확인 |

작업 간 의존성을 고려하여 일부 영역은 병렬로 진행할 수 있습니다.
