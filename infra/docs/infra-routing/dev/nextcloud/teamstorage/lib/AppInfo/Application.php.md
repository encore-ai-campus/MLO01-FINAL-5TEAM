# 파일 화면에 SSD 표시 등록

- 경로: `srcs/dev/nextcloud/teamstorage/lib/AppInfo/Application.php`
- 시그니처: `Application::__construct()`, `register(IRegistrationContext $context)`, `boot(IBootContext $context)`
- 파라미터: 파일 앱의 `LoadAdditionalScriptsEvent`
- 호출: `IEventDispatcher::addListener()`, `Util::addScript()`, `Util::addStyle()`
- 기능: 파일 화면에 `sidebar.js`와 `sidebar.css`를 불러온다.
