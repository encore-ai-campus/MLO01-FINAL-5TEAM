(() => {
    const quotaSelector = '[data-cy-files-navigation-settings-quota]';
    const format = new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 1 });
    const title = document.createElement('strong');
    const remaining = document.createElement('span');
    const bar = document.createElement('div');
    const fill = document.createElement('span');
    const usage = document.createElement('small');
    title.textContent = '공용 SSD';
    remaining.textContent = '확인 중';
    bar.className = 'teamstorage-space__meter';
    bar.setAttribute('role', 'meter');
    bar.setAttribute('aria-label', '공용 SSD 사용량');
    bar.setAttribute('aria-valuemin', '0');
    bar.setAttribute('aria-valuemax', '100');
    bar.append(fill);
    bar.hidden = true;
    let row;
    let pending = false;

    function capacity(bytes) {
        const useGB = bytes >= 1e9;
        return format.format(bytes / (useGB ? 1e9 : 1e6)) + (useGB ? ' GB' : ' MB');
    }

    async function refresh() {
        if (pending || document.hidden || !row?.isConnected) return;
        pending = true;
        try {
            const response = await fetch(OC.generateUrl('/apps/teamstorage/space'), {
                headers: { requesttoken: document.head.dataset.requesttoken, Accept: 'application/json' },
                cache: 'no-store',
                credentials: 'same-origin',
            });
            if (!response.ok) throw new Error('unavailable');
            const { totalBytes, freeBytes } = await response.json();
            if (!Number.isSafeInteger(totalBytes) || totalBytes <= 0 ||
                !Number.isSafeInteger(freeBytes) || freeBytes < 0 || freeBytes > totalBytes) {
                throw new Error('unavailable');
            }
            const used = totalBytes - freeBytes;
            remaining.textContent = capacity(freeBytes) + ' 남음';
            usage.textContent = capacity(totalBytes) + ' 중 ' + capacity(used) + ' 사용';
            const percent = (100 * used / totalBytes).toFixed(2);
            fill.style.width = percent + '%';
            bar.setAttribute('aria-valuenow', percent);
            bar.setAttribute('aria-valuetext', usage.textContent);
            bar.hidden = false;
        } catch {
            remaining.textContent = '용량 확인 불가';
            usage.textContent = '';
            bar.hidden = true;
        } finally {
            pending = false;
        }
    }

    function mount() {
        if (row?.isConnected) return;
        const quota = document.querySelector(quotaSelector);
        if (!quota) return;
        const anchor = quota.closest('li') || quota;
        row = document.createElement(anchor.tagName === 'LI' ? 'li' : 'div');
        row.className = 'teamstorage-space';
        row.title = 'Nextcloud 데이터가 저장된 서버 SSD의 여유 공간 (10분마다 갱신)';
        const header = document.createElement('div');
        header.className = 'teamstorage-space__header';
        header.append(title, remaining);
        row.append(header, bar, usage);
        anchor.before(row);
        refresh();
    }

    const observer = new MutationObserver(mount);
    observer.observe(document.documentElement, { childList: true, subtree: true });
    mount();
    window.setInterval(refresh, 10 * 60 * 1000);
    document.addEventListener('visibilitychange', refresh);
})();
