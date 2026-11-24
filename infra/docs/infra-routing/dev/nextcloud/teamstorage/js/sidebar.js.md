# SSD 사용량 막대

- 경로: `srcs/dev/nextcloud/teamstorage/js/sidebar.js`
- 시그니처: `mount()`, `refresh()`, `capacity(bytes)`
- 파라미터: 파일 앱의 개인 사용량 항목, 서버의 `totalBytes`, `freeBytes`
- 호출: `fetch()`, `MutationObserver`, `setInterval()`
- 기능: 개인 사용량 위에 공용 SSD 막대를 추가한다. 화면을 열거나 탭으로 돌아올 때, 화면을 보는 동안 10분마다 수치를 갱신한다.
