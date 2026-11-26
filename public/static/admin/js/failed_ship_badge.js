/**
 * 发货管理「审核失败」菜单数字角标（主框架父页面）。
 * 只读轮询 Ship/failedShipBadgeCount，不修改申请、审批或通知状态。
 * 与 CrmFailedOrderReminder 完全隔离：独立对象、class、timer、generation。
 */
(function (root, factory) {
    var api = factory();
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }
    if (root) {
        root.CrmFailedShipBadge = api;
    }
})(typeof window !== 'undefined' ? window : this, function () {
    'use strict';

    var POLL_MS = 60000;
    var BACKOFF_MAX = 300000;
    var REQUEST_TIMEOUT = 15000;
    var BADGE_CLASS = 'crm-failed-ship-badge';
    var HIDDEN_CLASS = 'crm-failed-ship-hidden';
    var MENU_ITEM_CLASS = 'crm-failed-ship-menu-item';

    var started = false;
    var stopped = false;
    var isPolling = false;
    var refreshQueued = false;
    var failStreak = 0;
    var pollTimer = null;
    var pollGeneration = 0;
    var pageAdminId = 0;
    var requestUrl = '';
    var $ = null;
    var identityNoticeShown = false;
    var authExpiredNoticeShown = false;
    var IDENTITY_NOTICE = '检测到当前登录账号已切换，请刷新此 CRM 页面后继续使用。';
    var AUTH_EXPIRED_NOTICE = '登录状态已失效，请重新登录或刷新页面。';

    /** 与 Ship::failedShipBadgeCount / Service 转发的固定 Session 失效契约（严格枚举） */
    var AUTH_EXPIRED_MSGS = {
        '登录状态已失效，请重新登录': true,
        '请先登录': true,
        '登录状态无效': true
    };

    function strictPositiveInt(value) {
        if (typeof value === 'number') {
            if (!isFinite(value) || value < 1 || Math.floor(value) !== value || value > 9007199254740991) {
                return null;
            }
            return value;
        }
        if (typeof value !== 'string' || !/^[1-9]\d*$/.test(value)) {
            return null;
        }
        var parsed = parseInt(value, 10);
        if (!isFinite(parsed) || parsed < 1 || parsed > 9007199254740991 || String(parsed) !== value) {
            return null;
        }
        return parsed;
    }

    function strictNonNegativeInt(value) {
        if (typeof value === 'number') {
            if (!isFinite(value) || value < 0 || Math.floor(value) !== value || value > 9007199254740991) {
                return null;
            }
            return value;
        }
        if (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value)) {
            return null;
        }
        var parsed = parseInt(value, 10);
        if (!isFinite(parsed) || parsed < 0 || parsed > 9007199254740991 || String(parsed) !== value) {
            return null;
        }
        return parsed;
    }

    function normalizeMenuPath(url) {
        var raw = String(url == null ? '' : url);
        var path = raw.split('?')[0].split('#')[0];
        path = path.replace(/^https?:\/\/[^/]+/i, '');
        path = path.replace(/\\/g, '/');
        path = path.replace(/\/+/g, '/');
        return path.toLowerCase().replace(/_/g, '');
    }

    function isFailedShipMenuUrl(url) {
        var normalized = normalizeMenuPath(url);
        if (!normalized) {
            return false;
        }
        // 必须匹配 Ship/failedShip 路由，禁止仅凭中文或误匹配 failedindex / 审核失败订单
        return /(?:^|\/)ship\/failedship(?:\.html)?$/.test(normalized);
    }

    function failedShipAnchors() {
        var matched = [];
        if (!document || !document.querySelectorAll) {
            return matched;
        }
        var anchors = document.querySelectorAll('#admin-navbar-side a[data-url]');
        var i;
        for (i = 0; i < anchors.length; i++) {
            var anchor = anchors[i];
            var href = anchor.getAttribute('data-url') || '';
            if (!isFailedShipMenuUrl(href)) {
                continue;
            }
            var cite = anchor.querySelector('cite');
            matched.push({
                anchor: anchor,
                cite: cite,
                href: href
            });
        }
        return matched;
    }

    function hasFailedShipMenu() {
        return failedShipAnchors().length > 0;
    }

    function menuBadgeLabel(total) {
        if (!(total > 0)) {
            return '0';
        }
        if (total >= 100) {
            return '99+';
        }
        return String(total);
    }

    function ensureMenuBadge() {
        var nodes = failedShipAnchors();
        var i;
        for (i = 0; i < nodes.length; i++) {
            var anchor = nodes[i].anchor;
            if ((' ' + anchor.className + ' ').indexOf(' ' + MENU_ITEM_CLASS + ' ') === -1) {
                anchor.className += (anchor.className ? ' ' : '') + MENU_ITEM_CLASS;
            }
            if (!anchor.querySelector('.' + BADGE_CLASS)) {
                var badge = document.createElement('span');
                badge.className = BADGE_CLASS + ' ' + HIDDEN_CLASS;
                if (nodes[i].cite && nodes[i].cite.insertAdjacentElement) {
                    nodes[i].cite.insertAdjacentElement('afterend', badge);
                } else {
                    anchor.appendChild(badge);
                }
            }
        }
        return nodes.length > 0;
    }

    function clearBadges() {
        if (!document || !document.querySelectorAll) {
            return;
        }
        var menus = document.querySelectorAll('#admin-navbar-side .' + BADGE_CLASS);
        var i;
        for (i = 0; i < menus.length; i++) {
            menus[i].textContent = '0';
            if ((' ' + menus[i].className + ' ').indexOf(' ' + HIDDEN_CLASS + ' ') === -1) {
                menus[i].className += ' ' + HIDDEN_CLASS;
            }
            if (menus[i].getAttribute('title')) {
                menus[i].removeAttribute('title');
            }
        }
    }

    function updateBadges(total) {
        if (!ensureMenuBadge()) {
            return false;
        }
        var menus = document.querySelectorAll('#admin-navbar-side .' + BADGE_CLASS);
        var label = menuBadgeLabel(total);
        var i;
        for (i = 0; i < menus.length; i++) {
            menus[i].textContent = label;
            if (total >= 100) {
                menus[i].setAttribute('title', '待处理 ' + total + ' 笔');
            } else if (menus[i].getAttribute('title')) {
                menus[i].removeAttribute('title');
            }
            if (total > 0) {
                menus[i].className = menus[i].className.replace(new RegExp('\\s*' + HIDDEN_CLASS, 'g'), '');
            } else if ((' ' + menus[i].className + ' ').indexOf(' ' + HIDDEN_CLASS + ' ') === -1) {
                menus[i].className += ' ' + HIDDEN_CLASS;
            }
        }
        return true;
    }

    function trimText(text) {
        return String(text == null ? '' : text).replace(/^\uFEFF/, '').replace(/^\s+|\s+$/g, '');
    }

    function isLoginDocument(text) {
        var sample = trimText(text).slice(0, 3000).toLowerCase();
        if (!sample || sample.charAt(0) !== '<') {
            return false;
        }
        return sample.indexOf('password') !== -1 || sample.indexOf('登录') !== -1 || sample.indexOf('login') !== -1;
    }

    /**
     * 严格识别本接口 Session 失效 JSON：固定 code=-200 + 固定 msg 枚举。
     * 禁止 code!==0 一律失效，禁止仅凭含“登录”二字判断。
     */
    function isAuthExpiredPayload(json) {
        if (!json || typeof json !== 'object' || Array.isArray(json)) {
            return false;
        }
        var code = json.code;
        if (code !== -200 && code !== '-200') {
            return false;
        }
        var msg = trimText(json.msg);
        return AUTH_EXPIRED_MSGS[msg] === true;
    }

    function parseResponse(text, boundAdminId) {
        var raw = trimText(text);
        if (!raw || raw.charAt(0) === '<') {
            return { ok: false, authHtml: isLoginDocument(raw), authExpired: false };
        }
        var json;
        try {
            json = JSON.parse(raw);
        } catch (e) {
            return { ok: false, authHtml: false, authExpired: false };
        }
        if (!json || typeof json !== 'object' || Array.isArray(json)) {
            return { ok: false, authHtml: false, authExpired: false };
        }
        if (json.code !== 0 && json.code !== '0') {
            if (isAuthExpiredPayload(json)) {
                return { ok: false, authHtml: false, authExpired: true };
            }
            return { ok: false, authHtml: false, authExpired: false };
        }
        var data = json.data;
        if (!data || typeof data !== 'object' || Array.isArray(data)) {
            return { ok: false, authHtml: false, authExpired: false };
        }
        var expectedId = boundAdminId != null ? strictPositiveInt(boundAdminId) : pageAdminId;
        if (expectedId === null) {
            expectedId = pageAdminId;
        }
        var remoteId = strictPositiveInt(data.admin_id);
        if (remoteId === null || expectedId < 1 || remoteId !== expectedId) {
            return { ok: false, authHtml: false, authExpired: false, identityFailed: true };
        }
        var total = strictNonNegativeInt(data.total);
        if (total === null) {
            return { ok: false, authHtml: false, authExpired: false };
        }
        return { ok: true, total: total, adminId: remoteId };
    }

    function stopPollingState() {
        pollGeneration += 1;
        stopped = true;
        refreshQueued = false;
        if (pollTimer) {
            clearTimeout(pollTimer);
            pollTimer = null;
        }
        clearBadges();
    }

    function stopForIdentityMismatch() {
        stopPollingState();
        if (identityNoticeShown) {
            return;
        }
        identityNoticeShown = true;
        try {
            if (typeof layui !== 'undefined' && layui.layer && typeof layui.layer.msg === 'function') {
                layui.layer.msg(IDENTITY_NOTICE);
            }
        } catch (ignoreNotice) {}
    }

    function stopForAuthExpired() {
        stopPollingState();
        if (authExpiredNoticeShown) {
            return;
        }
        authExpiredNoticeShown = true;
        try {
            if (typeof layui !== 'undefined' && layui.layer && typeof layui.layer.msg === 'function') {
                layui.layer.msg(AUTH_EXPIRED_NOTICE);
            }
        } catch (ignoreNotice) {}
    }

    function onFail(authHtml) {
        failStreak += 1;
        if (authHtml && failStreak < 3) {
            failStreak = 3;
        }
        // Session 失效 HTML：隐藏旧角标，不得保留旧账号可信数字
        if (authHtml) {
            clearBadges();
        }
    }

    function nextDelay() {
        if (failStreak <= 0) {
            return POLL_MS;
        }
        var delay = POLL_MS * Math.pow(2, failStreak);
        if (delay > BACKOFF_MAX) {
            return BACKOFF_MAX;
        }
        return delay;
    }

    function schedule(delay) {
        if (stopped) {
            return;
        }
        if (pollTimer) {
            clearTimeout(pollTimer);
        }
        pollTimer = setTimeout(tick, delay);
    }

    function tick() {
        pollTimer = null;
        if (stopped || isPolling || !$ || !requestUrl) {
            return;
        }
        if (typeof document !== 'undefined' && document.hidden) {
            return;
        }
        if (!hasFailedShipMenu()) {
            // 菜单暂不可见：不发请求；稍后有限重试，避免无意义长期轮询
            failStreak = 0;
            schedule(POLL_MS);
            return;
        }
        isPolling = true;
        var generation = pollGeneration;
        $.ajax({
            url: requestUrl,
            type: 'GET',
            dataType: 'text',
            cache: false,
            timeout: REQUEST_TIMEOUT,
            success: function (text) {
                if (stopped || generation !== pollGeneration) {
                    return;
                }
                var parsed = parseResponse(text);
                if (stopped || generation !== pollGeneration) {
                    return;
                }
                if (parsed.identityFailed) {
                    stopForIdentityMismatch();
                    return;
                }
                if (parsed.authExpired) {
                    stopForAuthExpired();
                    return;
                }
                if (!parsed.ok) {
                    // HTML 登录页：清角标并停止，避免旧数量残留与无效轮询
                    if (parsed.authHtml) {
                        stopForAuthExpired();
                        return;
                    }
                    onFail(false);
                    return;
                }
                failStreak = 0;
                updateBadges(parsed.total);
            },
            error: function (xhr) {
                if (stopped || generation !== pollGeneration) {
                    return;
                }
                var authHtml = false;
                try {
                    authHtml = isLoginDocument(xhr && xhr.responseText);
                } catch (e) {}
                if (authHtml) {
                    stopForAuthExpired();
                    return;
                }
                onFail(false);
            },
            complete: function () {
                isPolling = false;
                if (stopped || generation !== pollGeneration) {
                    return;
                }
                if (typeof document !== 'undefined' && document.hidden) {
                    // 隐藏期间不调度；可见性恢复时 onVisibility 会同步一次
                    return;
                }
                var immediate = refreshQueued;
                refreshQueued = false;
                schedule(immediate ? 0 : nextDelay());
            }
        });
    }

    function onVisibility() {
        if (stopped) {
            return;
        }
        if (document.hidden) {
            if (pollTimer) {
                clearTimeout(pollTimer);
                pollTimer = null;
            }
            return;
        }
        if (!isPolling) {
            // 可见性恢复即同步一次；清空可能残留的排队标志，避免紧随其后的 0ms 二次请求
            refreshQueued = false;
            if (pollTimer) {
                clearTimeout(pollTimer);
                pollTimer = null;
            }
            tick();
        }
    }

    function pauseTimers() {
        if (pollTimer) {
            clearTimeout(pollTimer);
            pollTimer = null;
        }
    }

    function stop() {
        pollGeneration += 1;
        stopped = true;
        refreshQueued = false;
        pauseTimers();
        clearBadges();
    }

    /**
     * 主动刷新：仅当确有进行中请求时置 refreshQueued。
     * 空闲时清队列标志并直接 tick，避免 complete 再发一次 0ms 请求。
     */
    function refreshNow() {
        if (stopped) {
            return;
        }
        if (typeof document !== 'undefined' && document.hidden) {
            if (isPolling) {
                refreshQueued = true;
            }
            return;
        }
        if (isPolling) {
            refreshQueued = true;
            return;
        }
        refreshQueued = false;
        if (pollTimer) {
            clearTimeout(pollTimer);
            pollTimer = null;
        }
        tick();
    }

    function start(options) {
        if (started) {
            return false;
        }
        options = options || {};
        var adminId = strictPositiveInt(options.adminId);
        if (adminId === null) {
            return false;
        }
        if (!options.url || !options.jquery) {
            return false;
        }
        if (!hasFailedShipMenu()) {
            return false;
        }
        $ = options.jquery;
        requestUrl = String(options.url);
        pageAdminId = adminId;
        started = true;
        stopped = false;
        if (typeof window !== 'undefined') {
            window.crmRefreshFailedShipBadge = refreshNow;
        }
        ensureMenuBadge();
        if (typeof document !== 'undefined') {
            document.addEventListener('visibilitychange', onVisibility);
        }
        if (typeof window !== 'undefined') {
            window.addEventListener('pagehide', pauseTimers);
        }
        tick();
        return true;
    }

    return {
        start: start,
        stop: stop,
        refreshNow: refreshNow,
        hasFailedShipMenu: hasFailedShipMenu,
        isFailedShipMenuUrl: isFailedShipMenuUrl,
        normalizeMenuPath: normalizeMenuPath,
        menuBadgeLabel: menuBadgeLabel,
        parseResponse: parseResponse,
        isAuthExpiredPayload: isAuthExpiredPayload,
        strictPositiveInt: strictPositiveInt,
        strictNonNegativeInt: strictNonNegativeInt,
        isLoginDocument: isLoginDocument
    };
});
