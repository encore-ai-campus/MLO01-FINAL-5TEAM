<?php

declare(strict_types=1);

namespace OCA\TeamStorage\Controller;

use OCP\AppFramework\Controller;
use OCP\AppFramework\Http\Attribute\FrontpageRoute;
use OCP\AppFramework\Http\Attribute\NoAdminRequired;
use OCP\AppFramework\Http\JSONResponse;
use OCP\IAppConfig;
use OCP\IRequest;

final class SpaceController extends Controller {
    // Allow two minutes for a delayed collection after the ten-minute interval.
    private const MAX_SAMPLE_AGE_SECONDS = 12 * 60;

    public function __construct(string $appName, IRequest $request, private IAppConfig $config) {
        parent::__construct($appName, $request);
    }

    #[NoAdminRequired]
    #[FrontpageRoute(verb: 'GET', url: '/space')]
    public function show(): JSONResponse {
        $sample = $this->config->getValueString('teamstorage', 'host_space');
        $result = ['totalBytes' => null, 'freeBytes' => null];
        if (preg_match('/^(\d{10}):(\d{1,16}):(\d{1,16})$/D', $sample, $parts)) {
            $age = time() - (int)$parts[1];
            $total = (int)$parts[2];
            $free = (int)$parts[3];
            if ($age >= 0 && $age <= self::MAX_SAMPLE_AGE_SECONDS && $total > 0 && $free <= $total) {
                $result = ['totalBytes' => $total, 'freeBytes' => $free];
            }
        }

        return new JSONResponse($result, headers: ['Cache-Control' => 'private, no-store']);
    }
}
