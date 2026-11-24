# SSD 측정값 조회

- 경로: `srcs/dev/nextcloud/teamstorage/lib/Controller/SpaceController.php`
- 시그니처: `SpaceController::__construct(string $appName, IRequest $request, IAppConfig $config)`, `show(): JSONResponse`
- 파라미터: 로그인한 사용자의 `GET /apps/teamstorage/space`, `teamstorage/host_space`
- 호출: `IAppConfig::getValueString()`, `JSONResponse`
- 기능: 최근 12분 이내의 유효한 전체·여유 바이트를 반환한다. 측정값이 오래되었거나 유효하지 않으면 두 값은 `null`이다.
