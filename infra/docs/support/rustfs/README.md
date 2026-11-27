# 팀 공용 RustFS 사용법

RustFS는 팀 개발 환경에서 Spark·Delta가 S3 API로 파일을 읽고 쓰는 데 사용하는 저장소입니다. 83번 공용 서버를 이용하는 팀원은 1번, 서버를 관리하는 사람은 2번, 자기 PC에 별도 저장소를 띄우려는 사람은 3번을 참고하세요.

## 1. 83번 서버 접속

| 용도 | 주소 | 접속 위치 |
|---|---|---|
| S3 API | `http://192.168.0.83:9000` | 같은 내부망의 팀원 PC에서 Spark 또는 S3 클라이언트의 엔드포인트로 지정 |
| 관리 화면 | `http://localhost:9001` | 마지막 확인 기준으로 83번 PC의 브라우저에서 접속 |

팀원 PC의 PowerShell에서 아래 명령으로 연결을 확인합니다.

```powershell
curl.exe -i --connect-timeout 5 http://192.168.0.83:9000/health/ready
```

`200 OK`가 나오면 해당 PC에서 저장소에 연결된 것입니다. 파일을 읽고 쓰려면 해당 버킷에 권한이 있는 접근 키가 필요합니다. 마지막 확인 때는 83번 Ubuntu 안에서 준비 상태가 정상이었으며, 팀원 PC에서의 접속은 별도 확인이 필요합니다.

## 2. 기존 서버 켜기·끄기

83번의 WSL 배포판은 `Ubuntu-24.04`이고 기존 컨테이너 이름은 `rustfs`입니다. 아래 명령은 83번 **Ubuntu 터미널**에서 실행합니다.

- 켜기 — `sudo docker start rustfs`
- 끄기 — `sudo docker stop rustfs`

중지하면 팀원 접속도 끊기므로 사용 중인 작업이 끝난 뒤 끕니다. 중지한 컨테이너를 다시 시작하면 기존 데이터를 그대로 사용합니다. 시작 후에는 아래 명령으로 저장소가 요청을 받을 준비가 됐는지 확인합니다.

```bash
curl -i http://localhost:9000/health/ready
```

계속 응답하지 않으면 `sudo docker logs --tail 50 rustfs`로 시작 로그를 확인합니다. 여기서는 `200 OK`인데 팀원 PC에서 연결 시간이 초과되면 Windows 방화벽과 WSL 포트 전달 설정을 확인합니다.

## 3. 새 PC에 설치

Docker와 Compose가 설치된 Ubuntu에서 팀 저장소를 열고, 저장소 최상위 폴더에서 아래 명령을 실행합니다. `.env`는 [compose.yaml](../../../srcs/support/rustfs/compose.yaml)과 같은 폴더에 둡니다.

```bash
cd infra/srcs/support/rustfs
nano .env
```

아래 두 줄을 입력하고 예시 값은 직접 정한 ID와 긴 비밀번호로 바꿉니다. `Ctrl+O`, `Enter`, `Ctrl+X` 순서로 저장하고 나옵니다. 실제 자격 증명이 담긴 `.env`는 Git에 올리지 않습니다.

```dotenv
RUSTFS_ACCESS_KEY=your-admin-id
RUSTFS_SECRET_KEY=your-long-random-password
```

같은 폴더에서 기동한 뒤 준비 상태를 확인합니다.

```bash
sudo docker compose up -d
curl -i http://localhost:9000/health/ready
```

응답이 `200 OK`이면 새 PC의 브라우저에서 `http://localhost:9001`을 열고 `.env`에 적은 값으로 로그인합니다. 새 설치의 데이터는 그 PC의 `rustfs-data` 볼륨에 저장됩니다. 83번의 버킷과 파일은 공용 서버 주소로 접속해야 볼 수 있습니다.
