# VMware Workstation 및 Ubuntu 개발 환경 구축

> 작성일: 2026-11-16
> 작성자: 조성현
> 관련 이슈: [#2 VMware Workstation 설치 및 Ubuntu 가상머신 환경 설정 계획](https://github.com/encore-ai-campus/MLO01-FINAL-5TEAM/issues/2)

---

## 1. 개요

### 1.1 목적

팀원별 개발 환경을 통일하기 위해 VMware Workstation 기반 Ubuntu 24.04 LTS 가상머신을 구축합니다.

Windows 운영체제를 유지하면서 독립적인 Linux 개발 환경을 제공하고, 네트워크·SSH·공유 폴더 설정을 표준화하여 팀원 간 환경 차이를 최소화합니다.

기본 환경 구성 및 검증이 완료되면 VMware 스냅샷을 생성하여 안정적인 복원 지점을 확보합니다.

### 1.2 구축 범위

- VMware Workstation 설치 및 Ubuntu 가상머신 생성
- 가상머신 CPU·RAM·디스크 자원 할당
- NAT 및 Host-Only 네트워크 구성
- OpenSSH 기반 원격 접속 환경 구축
- VMware Tools 설치 및 공유 폴더 연결
- 공유 폴더 자동 마운트 설정
- 기본 환경 검증 및 VMware 스냅샷 생성

> Python, Java, Kafka, Spark, PostgreSQL, Redis 등의 설치 및 버전 통일은 별도 `infra/srcs/dev/environment/`에서 관리합니다.

---

## 2. 환경 구성

### 2.1 기본 환경

| 항목                | 기준                    |
| ----------------- | --------------------- |
| Host OS           | Windows               |
| Virtualization    | VMware Workstation    |
| Guest OS          | Ubuntu 24.04 LTS      |
| CPU Architecture  | x86-64 (AMD64)        |
| Network Adapter 1 | NAT                   |
| Network Adapter 2 | Host-Only             |
| Remote Access     | OpenSSH               |
| File Sharing      | VMware Shared Folders |
| Recovery          | VMware Snapshot       |

### 2.2 가상머신 자원 할당

개발 및 통합 테스트를 위해 다음과 같이 가상머신 자원을 할당합니다.

| 항목                  | 설정         |
| ------------------- | ---------- |
| Processors          | 1          |
| Cores per Processor | 6          |
| Total vCPU          | **6 vCPU** |
| Memory              | **16GB**   |
| Disk                | **150GB**  |

위 설정을 기준으로 가상머신을 구성하며, 팀원별 PC 사양 및 실제 테스트 부하에 따라 자원을 조정할 수 있습니다.

**가상머신 자원 변경 시 참고 사항**

- CPU 및 RAM은 가상머신 종료 후 VMware 설정에서 변경할 수 있습니다.
- 디스크 용량은 확장할 수 있으나, 스냅샷 상태에 따라 확장 작업이 제한될 수 있습니다.
- 디스크 확장 후 Ubuntu 내부의 파티션 및 파일시스템 확장이 추가로 필요할 수 있습니다.
- RAM 16GB 할당 시 Windows 호스트의 메모리 여유를 확인해야 합니다.

---

## 3. VMware 설치 및 가상머신 생성

### 3.1 VMware Workstation 설치

1. VMware Workstation 설치 파일을 다운로드합니다.
2. Windows 환경에 VMware Workstation을 설치합니다.
3. 설치 완료 후 프로그램의 정상 실행 여부를 확인합니다.

### 3.2 Ubuntu 가상머신 생성

1. Ubuntu 24.04 LTS ISO 파일을 다운로드합니다.
2. VMware Workstation에서 새로운 가상머신을 생성합니다.
3. Ubuntu ISO 파일을 지정합니다.
4. CPU, RAM, 디스크 자원을 할당합니다.
5. Ubuntu 설치를 진행합니다.
6. 설치 완료 후 정상 부팅을 확인합니다.

Ubuntu 버전 확인:

```bash
lsb_release -a
uname -m
```

예상 결과:

- Ubuntu 24.04 LTS
- x86_64

---

## 4. 네트워크 환경 설정

### 4.1 네트워크 구성

가상머신에는 NAT와 Host-Only 네트워크를 함께 사용합니다.

| 네트워크      | 목적                              |
| --------- | ------------------------------- |
| NAT       | Ubuntu에서 인터넷 및 외부 패키지 저장소 접근    |
| Host-Only | Windows Host와 Ubuntu VM 간 내부 통신 |

### 4.2 VMware 네트워크 설정

VMware Workstation의 가상머신 설정에서 네트워크 어댑터를 구성합니다.

- Network Adapter 1: NAT
- Network Adapter 2: Host-Only

Host-Only 네트워크는 일반적으로 VMnet1을 사용하지만, 실제 VMware 네트워크 설정을 확인하여 적용합니다.

### 4.3 네트워크 연결 확인

Ubuntu에서 네트워크 인터페이스 및 IP 주소를 확인합니다.

```bash
ip -br addr
ip route
```

외부 네트워크 연결 확인:

```bash
ping -c 4 1.1.1.1
```

DNS 확인:

```bash
getent hosts ubuntu.com
```

**검증 항목**

- NAT 인터페이스에 IP 주소가 할당되었는가?
- Ubuntu에서 외부 인터넷에 접근할 수 있는가?
- Host-Only 인터페이스에 내부 IP가 할당되었는가?
- Windows에서 Ubuntu의 Host-Only IP로 통신할 수 있는가?

---

## 5. SSH 원격 접속 설정

### 5.1 OpenSSH 설치

Ubuntu에서 OpenSSH Server를 설치합니다.

```bash
sudo apt update
sudo apt install -y openssh-server
```

SSH 서비스를 활성화합니다.

```bash
sudo systemctl enable --now ssh
```

실행 상태를 확인합니다.

```bash
sudo systemctl status ssh
```

### 5.2 Windows에서 SSH 접속

Windows PowerShell에서 Ubuntu의 Host-Only IP를 사용하여 접속합니다.

```powershell
ssh <Ubuntu사용자명>@<Host-Only-IP>
```

접속에 성공하면 Ubuntu 터미널을 Windows PowerShell에서 사용할 수 있습니다.

**검증 항목**

- SSH 서비스가 정상적으로 실행되는가?
- Windows에서 Ubuntu로 SSH 접속이 가능한가?
- SSH 접속 후 Ubuntu 명령어가 정상 실행되는가?

---

## 6. VMware Tools 및 공유 폴더 설정

### 6.1 VMware Tools 설치

Ubuntu에서 `open-vm-tools`를 설치합니다.

```bash
sudo apt update
sudo apt install -y open-vm-tools open-vm-tools-desktop
```

`open-vm-tools-desktop`은 GUI 환경에서 사용하는 구성 요소입니다.

필요한 경우 Ubuntu를 재부팅합니다.

### 6.2 Windows 공유 폴더 생성

Windows에 VMware 공유 폴더로 사용할 디렉터리를 생성합니다.

예시:

```text
C:\VM-Share
```

VMware Workstation에서 다음 순서로 설정합니다.

1. VM Settings 진입
2. Options 선택
3. Shared Folders 선택
4. Always enabled 활성화
5. Add를 통해 Windows 공유 폴더 등록
6. Share Name 지정

### 6.3 Ubuntu 공유 폴더 마운트

Ubuntu에서 마운트 경로를 준비합니다.

```bash
sudo mkdir -p /mnt/hgfs
```

이미 자동 마운트되어 있는지 확인합니다.

```bash
findmnt /mnt/hgfs
ls /mnt/hgfs
```

마운트되어 있지 않은 경우 다음 명령어를 사용할 수 있습니다.

```bash
sudo vmhgfs-fuse .host:/ /mnt/hgfs -o allow_other,uid=$(id -u),gid=$(id -g)
```

공유 폴더를 확인합니다.

```bash
ls -la /mnt/hgfs
```

### 6.4 공유 폴더 자동 마운트

재부팅 이후에도 공유 폴더를 사용할 수 있도록 `/etc/fstab`에 자동 마운트 설정을 적용합니다.

사용자 UID 및 GID 확인:

```bash
id -u
id -g
```

`/etc/`**`fstab`** 설정 예시:

```fstab
.host:/ /mnt/hgfs fuse.vmhgfs-fuse defaults,allow_other,_netdev,uid=1000,gid=1000 0 0
```

> 위 UID와 GID는 예시입니다. 실제 Ubuntu 사용자 값으로 변경해야 하며, 이미 동일한 마운트가 설정되어 있다면 중복 등록하지 않습니다.

설정 후 가상머신을 재부팅하고 자동 마운트 여부를 확인합니다.

```bash
findmnt /mnt/hgfs
ls /mnt/hgfs
```

### 6.5 공유 폴더 읽기·쓰기 검증

Windows에서 생성한 테스트 파일을 Ubuntu에서 확인합니다.

반대로 Ubuntu의 공유 폴더에 테스트 파일을 생성하여 Windows에서 확인합니다.

**검증 항목**

- Windows 공유 폴더가 Ubuntu에서 보이는가?
- Windows에서 생성한 파일을 Ubuntu에서 읽을 수 있는가?
- Ubuntu에서 생성한 파일을 Windows에서 읽을 수 있는가?
- Ubuntu에서 공유 폴더에 파일을 작성할 수 있는가?
- 재부팅 후에도 공유 폴더가 정상 마운트되는가?

---

## 7. VMware 스냅샷 관리

### 7.1 스냅샷 생성 목적

Ubuntu 기본 환경의 구축 및 검증이 완료된 상태를 보존하기 위해 VMware 스냅샷을 생성합니다.

스냅샷을 활용하여 추가 프로그램 설치나 환경 설정 중 문제가 발생하더라도 검증된 상태로 복원할 수 있도록 합니다.

### 7.2 스냅샷 생성 시점

다음 작업이 완료된 후 기본 환경 스냅샷을 생성합니다.

- Ubuntu 24.04 LTS 설치 완료
- NAT 및 Host-Only 네트워크 연결 검증
- SSH 원격 접속 검증
- VMware Tools 설치 및 검증
- 공유 폴더 읽기·쓰기 및 자동 마운트 검증

### 7.3 스냅샷 생성 절차

1. 실행 중인 작업을 종료하고 가능하면 가상머신을 정상 종료합니다.
2. VMware Workstation에서 Snapshot Manager를 실행합니다.
3. Take Snapshot을 선택합니다.
4. 스냅샷 이름과 설명을 기록합니다.
5. 스냅샷 생성 여부를 확인합니다.

스냅샷 이름 예시:

```text
ubuntu-24.04-base
```

### 7.4 Base VM 활용

검증된 Ubuntu 기본 환경을 Base VM으로 관리하고, 팀원별 환경 구성 시 참고합니다.

다른 PC에 가상머신을 배포할 경우 Full Clone 등의 방법을 검토할 수 있습니다.

가상머신 복제 시 다음 항목을 확인합니다.

- 사용자 계정 및 호스트명
- 네트워크 설정 및 IP 주소
- SSH 호스트 키
- VMware 공유 폴더의 Windows 경로

> VMware 스냅샷은 복원 지점이며, 별도의 백업을 대체하지 않습니다. 또한 스냅샷 파일 및 가상머신 디스크는 GitHub에 저장하지 않습니다.

---

## 8. 환경 검증 및 완료 기준

| 항목             | 검증 내용                    | 완료 기준    |
| -------------- | ------------------------ | -------- |
| VMware         | 프로그램 및 가상머신 실행           | 정상 실행    |
| Ubuntu         | Ubuntu 24.04 LTS 설치      | 정상 부팅    |
| CPU·RAM·Disk   | 6 vCPU / 16GB / 150GB 할당 | 설정 확인    |
| NAT            | 외부 네트워크 연결               | 정상 통신    |
| Host-Only      | Windows ↔ Ubuntu 통신      | 정상 통신    |
| SSH            | Windows PowerShell 원격 접속 | 접속 성공    |
| VMware Tools   | 설치 및 기능 확인               | 정상 동작    |
| Shared Folders | 파일 읽기·쓰기                 | 정상 동작    |
| Auto Mount     | 재부팅 후 공유 폴더 연결           | 정상 마운트   |
| Snapshot       | 기본 환경 스냅샷 생성             | 생성 확인    |
| Documentation  | 설치 및 설정 절차 기록            | 문서 작성 완료 |

### 최종 완료 기준

- 팀원별 VMware 기반 Ubuntu 개발 환경 구축
- NAT 및 Host-Only 네트워크 정상 연결
- Windows에서 Ubuntu로 SSH 원격 접속 가능
- Windows·Ubuntu 간 공유 폴더 읽기·쓰기 가능
- 공유 폴더 자동 마운트 정상 동작
- 기본 환경 검증 후 VMware 스냅샷 생성
- 환경 설정 및 검증 절차 문서화

---

## 9. 관련 코드 및 작업 범위

VMware 환경 구축에 사용하는 스크립트는 다음 디렉터리에서 관리합니다.

```text
infra/srcs/dev/vmware/scripts/
```

이 디렉터리에서는 VMware 가상머신 기본 설정 및 관리에 필요한 스크립트를 관리합니다.

프로그램 및 라이브러리 설치 자동화는 별도 디렉터리에서 관리합니다.

```text
infra/srcs/dev/environment/
```

이를 통해 **가상머신 기본 환경 구축과 개발 프로그램 설치 자동화를 분리**합니다.

---

## 10. 참고 사항

- 팀원별 하드웨어 사양에 따라 가상머신 자원 할당값이 달라질 수 있습니다.
- CPU 및 RAM은 가상머신 종료 후 변경할 수 있습니다.
- 디스크 확장은 스냅샷 상태 및 Ubuntu 파일시스템 구성을 고려하여 진행해야 합니다.
- Host-Only IP 및 Windows 공유 폴더 경로는 PC별로 다를 수 있습니다.
- 공유 폴더 자동 마운트 설정에는 실제 Ubuntu 사용자 UID·GID를 적용해야 합니다.
- 기본 환경 스냅샷은 개발 프로그램 설치 전에 생성하는 것을 권장합니다.
- 실제 작업 과정에서 변경된 설정과 검증 결과는 본 문서에 반영합니다.