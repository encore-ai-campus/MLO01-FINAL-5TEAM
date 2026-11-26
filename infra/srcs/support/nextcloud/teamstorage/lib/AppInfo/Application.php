<?php

declare(strict_types=1);

namespace OCA\TeamStorage\AppInfo;

use OCA\Files\Event\LoadAdditionalScriptsEvent;
use OCP\AppFramework\App;
use OCP\AppFramework\Bootstrap\IBootContext;
use OCP\AppFramework\Bootstrap\IBootstrap;
use OCP\AppFramework\Bootstrap\IRegistrationContext;
use OCP\EventDispatcher\IEventDispatcher;
use OCP\Util;

final class Application extends App implements IBootstrap {
    public const APP_ID = 'teamstorage';

    public function __construct() {
        parent::__construct(self::APP_ID);
    }

    public function register(IRegistrationContext $context): void {
    }

    public function boot(IBootContext $context): void {
        $context->injectFn(static function (IEventDispatcher $events): void {
            $events->addListener(LoadAdditionalScriptsEvent::class, static function (): void {
                Util::addScript(self::APP_ID, 'sidebar');
                Util::addStyle(self::APP_ID, 'sidebar');
            });
        });
    }
}
