/**
 * 审核失败订单提醒（主框架父页面）。
 * 只读轮询 /admin/index/failedOrderReminderSummary，不修改订单、通知或审核状态。
 * localStorage 只保存订单 ID、事件标识、提醒时间和多标签页锁，不保存拒绝原因和客户信息。
 */
(function (root, factory) {
    var api = factory();
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }
    if (root) {
        root.CrmFailedOrderReminder = api;
    }
})(typeof window !== 'undefined' ? window : this, function () {
    'use strict';

    var POLL_MS = 60000;
    var REPEAT_MS = 1800000;
    var LOCK_MS = 20000;
    var BACKOFF_MAX = 300000;
    var REQUEST_TIMEOUT = 15000;
    var FRESH_KEY = 'crm_failed_order_reminder_fresh_login';
    var TAB_ID = 't' + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);

    var started = false;
    var stopped = false;
    var isPolling = false;
    var popupOpen = false;
    var pendingShow = false;
    var remindAgainAfterClose = false;
    var refreshQueued = false;
    var failStreak = 0;
    var pollTimer = null;
    var deferredTimer = null;
    var renewTimer = null;
    var lockToken = '';
    var reminderLayerIndex = null;
    var lastPayload = null;
    var pageStartedAt = Date.now();
    var listenersReadyAt = 0;

    var $ = null;
    var layer = null;
    var element = null;
    var tabApi = null;
    var requestUrl = '';
    var pageAdminId = 0;
    var metaKey = '';
    var snapKey = '';
    var memoryMeta = null;
    var memorySnap = null;
    var pollGeneration = 0;
    var identityNoticeShown = false;
    var IDENTITY_NOTICE = '检测到当前登录账号已切换，请刷新此 CRM 页面后继续使用。';

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

    function eventKind(key) {
        if (typeof key !== 'string') {
            return '';
        }
        if (/^h[1-9]\d*$/.test(key)) {
            return 'h';
        }
        if (/^t[1-9]\d*$/.test(key)) {
            return 't';
        }
        if (key === 'u') {
            return 'u';
        }
        return '';
    }

    function isNewerReject(prevKey, nextKey) {
        if (prevKey === nextKey) {
            return false;
        }
        var prevKind = eventKind(prevKey);
        var nextKind = eventKind(nextKey);
        if (prevKind === 'h' && nextKind === 'h') {
            return parseInt(nextKey.slice(1), 10) > parseInt(prevKey.slice(1), 10);
        }
        if (prevKind === 't' && nextKind === 't') {
            return parseInt(nextKey.slice(1), 10) > parseInt(prevKey.slice(1), 10);
        }
        return false;
    }

    function normalizeEvents(raw) {
        if (Array.isArray(raw)) {
            return raw.length === 0 ? {} : null;
        }
        if (!raw || typeof raw !== 'object') {
            return null;
        }
        var out = {};
        var keys = Object.keys(raw);
        var i;
        for (i = 0; i < keys.length; i++) {
            var key = keys[i];
            if (!/^[1-9]\d*$/.test(key)) {
                return null;
            }
            var value = raw[key];
            if (typeof value !== 'string' || !eventKind(value)) {
                return null;
            }
            out[String(parseInt(key, 10))] = value;
        }
        return out;
    }

    function diffNewEvents(prev, current) {
        var list = [];
        if (!prev || !current) {
            return list;
        }
        var ids = Object.keys(current);
        var i;
        for (i = 0; i < ids.length; i++) {
            var id = ids[i];
            if (!Object.prototype.hasOwnProperty.call(prev, id)) {
                list.push(id);
                continue;
            }
            if (isNewerReject(prev[id], current[id])) {
                list.push(id);
            }
        }
        return list;
    }

    function decideReminder(input) {
        var current = input.currentSnapshot || {};
        var total = input.total;
        var now = input.now;
        var nextAllowedAt = input.nextAllowedAt || 0;
        var fresh = !!input.freshLogin;
        var busy = !!input.busy;
        var popupOpenNow = !!input.popupOpen;
        var locked = !!input.lockHeldByOther;
        var hasBaseline = !!input.hasBaseline;
        var newIds = hasBaseline ? diffNewEvents(input.prevSnapshot || {}, current) : [];

        function pack(action, reason, writeSnapshot, consumeFresh, retry, ids) {
            return {
                action: action,
                reason: reason,
                newIds: ids || [],
                writeSnapshot: !!writeSnapshot,
                consumeFresh: !!consumeFresh,
                retry: !!retry
            };
        }

        function blocked(reason, writeSnapshot, ids) {
            if (busy) {
                return pack('defer', reason, writeSnapshot, false, true, ids);
            }
            if (popupOpenNow) {
                return pack('skip', reason, writeSnapshot, false, false, ids);
            }
            if (locked) {
                return pack('skip', reason, writeSnapshot, false, true, ids);
            }
            return null;
        }

        if (!hasBaseline) {
            var shouldShow = total > 0 && (fresh || !nextAllowedAt || now >= nextAllowedAt);
            if (!shouldShow) {
                return pack('baseline', 'baseline', true, total === 0 && fresh, false, []);
            }
            var held = blocked(fresh ? 'fresh' : 'initial', true, []);
            if (held) {
                return held;
            }
            return pack('show', fresh ? 'fresh' : 'initial', false, false, false, []);
        }

        if (total <= 0) {
            return pack('skip', 'empty', true, fresh, false, []);
        }

        var reason = '';
        if (newIds.length) {
            reason = 'new';
        } else if (fresh) {
            reason = 'fresh';
        } else if (!nextAllowedAt || now >= nextAllowedAt) {
            reason = 'repeat';
        }

        if (!reason) {
            return pack('skip', 'cooldown', true, false, false, []);
        }

        var writeBeforeShow = reason !== 'new';
        var heldLater = blocked(reason, writeBeforeShow, newIds);
        if (heldLater) {
            return heldLater;
        }
        return pack('show', reason, false, false, false, newIds);
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function emptyMeta() {
        return { v: 1, lastShownAt: 0, nextAllowedAt: 0, consumedNonce: '', lock: null };
    }

    function emptySnap() {
        return { v: 1, events: null };
    }

    function readMeta() {
        if (!metaKey) {
            return memoryMeta ? cloneMeta(memoryMeta) : emptyMeta();
        }
        try {
            var raw = localStorage.getItem(metaKey);
            if (!raw) {
                return emptyMeta();
            }
            var data = JSON.parse(raw);
            if (!data || typeof data !== 'object' || data.v !== 1) {
                return emptyMeta();
            }
            var lock = null;
            if (data.lock && typeof data.lock === 'object') {
                var until = Number(data.lock.until) || 0;
                if (until > Date.now() && data.lock.owner && data.lock.token) {
                    lock = {
                        owner: String(data.lock.owner),
                        token: String(data.lock.token),
                        until: until
                    };
                }
            }
            return {
                v: 1,
                lastShownAt: Number(data.lastShownAt) || 0,
                nextAllowedAt: Number(data.nextAllowedAt) || 0,
                consumedNonce: typeof data.consumedNonce === 'string' ? data.consumedNonce : '',
                lock: lock
            };
        } catch (e) {
            return memoryMeta ? cloneMeta(memoryMeta) : emptyMeta();
        }
    }

    function writeMeta(meta) {
        var next = cloneMeta(meta);
        try {
            var latest = readMeta();
            if (!next.consumedNonce && latest.consumedNonce) {
                next.consumedNonce = latest.consumedNonce;
            }
            if ((latest.nextAllowedAt || 0) > (next.nextAllowedAt || 0)) {
                next.nextAllowedAt = latest.nextAllowedAt;
                next.lastShownAt = latest.lastShownAt;
            }
        } catch (ignoreLatest) {}
        memoryMeta = cloneMeta(next);
        if (!metaKey) {
            return;
        }
        try {
            localStorage.setItem(metaKey, JSON.stringify(next));
        } catch (e) {}
    }

    function cloneMeta(meta) {
        return {
            v: 1,
            lastShownAt: meta.lastShownAt || 0,
            nextAllowedAt: meta.nextAllowedAt || 0,
            consumedNonce: meta.consumedNonce || '',
            lock: meta.lock ? {
                owner: meta.lock.owner,
                token: meta.lock.token,
                until: meta.lock.until
            } : null
        };
    }

    function readSnap() {
        if (!snapKey) {
            return memorySnap ? { v: 1, events: memorySnap.events } : emptySnap();
        }
        try {
            var raw = localStorage.getItem(snapKey);
            if (!raw) {
                return emptySnap();
            }
            var data = JSON.parse(raw);
            if (!data || typeof data !== 'object' || data.v !== 1 || !Object.prototype.hasOwnProperty.call(data, 'events')) {
                return emptySnap();
            }
            if (data.events === null) {
                return emptySnap();
            }
            var events = normalizeEvents(data.events);
            if (events === null) {
                return emptySnap();
            }
            return { v: 1, events: events };
        } catch (e) {
            return emptySnap();
        }
    }

    function writeSnap(events) {
        memorySnap = { v: 1, events: events };
        if (!snapKey) {
            return;
        }
        try {
            localStorage.setItem(snapKey, JSON.stringify({ v: 1, events: events }));
        } catch (e) {}
    }

    function currentNonce() {
        try {
            return sessionStorage.getItem(FRESH_KEY) || '';
        } catch (e) {
            return '';
        }
    }

    function computeFresh(meta) {
        var nonce = currentNonce();
        if (!nonce) {
            return false;
        }
        if (meta && meta.consumedNonce && meta.consumedNonce === nonce) {
            return false;
        }
        return true;
    }

    function rememberNonce(meta) {
        var nonce = currentNonce();
        if (!nonce) {
            return meta;
        }
        meta.consumedNonce = nonce;
        try {
            sessionStorage.removeItem(FRESH_KEY);
        } catch (e) {}
        return meta;
    }

    function tryAcquireLock() {
        var meta = readMeta();
        var now = Date.now();
        if (meta.lock && meta.lock.until > now && meta.lock.owner !== TAB_ID) {
            return false;
        }
        var token = TAB_ID + ':' + now + ':' + Math.random().toString(36).slice(2);
        meta.lock = { owner: TAB_ID, token: token, until: now + LOCK_MS };
        writeMeta(meta);
        var again = readMeta();
        if (!again.lock || again.lock.token !== token) {
            return false;
        }
        lockToken = token;
        return true;
    }

    function renewLock() {
        if (!lockToken) {
            return;
        }
        var meta = readMeta();
        if (!meta.lock || meta.lock.token !== lockToken) {
            return;
        }
        meta.lock.until = Date.now() + LOCK_MS;
        writeMeta(meta);
    }

    function releaseLock() {
        if (!lockToken) {
            return;
        }
        var token = lockToken;
        lockToken = '';
        var meta = readMeta();
        if (meta.lock && meta.lock.token === token) {
            meta.lock = null;
            writeMeta(meta);
        }
    }

    function startLockRenew() {
        stopLockRenew();
        renewTimer = setTimeout(function renewChain() {
            renewLock();
            if (!popupOpen || !lockToken) {
                renewTimer = null;
                return;
            }
            renewTimer = setTimeout(renewChain, 8000);
        }, 8000);
    }

    function stopLockRenew() {
        if (renewTimer) {
            clearTimeout(renewTimer);
            renewTimer = null;
        }
    }

    function isBusinessLayer(el) {
        if (!el || typeof el.className !== 'string') {
            return false;
        }
        var cls = ' ' + el.className + ' ';
        if (cls.indexOf(' layui-layer ') === -1) {
            return false;
        }
        if (el.getAttribute && el.getAttribute('data-crm-failed-reminder') === '1') {
            return false;
        }
        if (cls.indexOf(' layui-layer-shade ') !== -1 ||
            cls.indexOf(' layui-layer-loading ') !== -1 ||
            cls.indexOf(' layui-layer-tips ') !== -1 ||
            cls.indexOf(' layui-layer-msg ') !== -1) {
            return false;
        }
        if (el.style && el.style.display === 'none') {
            return false;
        }
        return true;
    }

    function documentBusy(doc) {
        if (!doc || !doc.querySelectorAll) {
            return false;
        }
        var nodes = doc.querySelectorAll('.layui-layer');
        var i;
        for (i = 0; i < nodes.length; i++) {
            if (isBusinessLayer(nodes[i])) {
                return true;
            }
        }
        return false;
    }

    function isEditingBusy() {
        if (typeof organizations !== 'undefined' && organizations && organizations.length) {
            return true;
        }
        try {
            if (documentBusy(document)) {
                return true;
            }
        } catch (e) {
            return true;
        }
        var frames;
        try {
            frames = document.querySelectorAll('#admin-body iframe');
        } catch (e2) {
            return true;
        }
        var i;
        for (i = 0; i < frames.length; i++) {
            try {
                if (frames[i].contentDocument && documentBusy(frames[i].contentDocument)) {
                    return true;
                }
            } catch (e3) {
                return true;
            }
        }
        return false;
    }

    function failedOrderAnchors() {
        var matched = [];
        if (!document.querySelectorAll) {
            return matched;
        }
        var anchors = document.querySelectorAll('#admin-navbar-side a');
        var i;
        for (i = 0; i < anchors.length; i++) {
            var anchor = anchors[i];
            var cite = anchor.querySelector('cite');
            if (!cite) {
                continue;
            }
            var title = (cite.textContent || '').replace(/^\s+|\s+$/g, '');
            var href = anchor.getAttribute('data-url') || '';
            var hrefHit = /(?:^|\/)failedindex(?:\.html)?(?:\?|#|$)/i.test(href);
            if (title === '审核失败订单' || hrefHit) {
                matched.push({
                    anchor: anchor,
                    cite: cite,
                    title: title || '审核失败订单',
                    href: href
                });
            }
        }
        return matched;
    }

    function menuBadgeLabel(total) {
        if (!(total > 0)) {
            return '0';
        }
        if (total > 999) {
            return '99+';
        }
        return String(total);
    }

    function ensureMenuBadge() {
        var nodes = failedOrderAnchors();
        var i;
        for (i = 0; i < nodes.length; i++) {
            var anchor = nodes[i].anchor;
            if (anchor.className.indexOf('crm-failed-order-menu-item') === -1) {
                anchor.className += (anchor.className ? ' ' : '') + 'crm-failed-order-menu-item';
            }
            if (!anchor.querySelector('.crm-failed-order-badge')) {
                var badge = document.createElement('span');
                badge.className = 'crm-failed-order-badge crm-failed-order-hidden';
                nodes[i].cite.insertAdjacentElement('afterend', badge);
            }
        }
    }

    function updateBadges(total) {
        var count = total > 0 ? String(total) : '0';
        var bells = document.querySelectorAll('.crm-failed-order-bell');
        var i;
        for (i = 0; i < bells.length; i++) {
            if (total > 0) {
                bells[i].className = bells[i].className.replace(/\s*crm-failed-order-hidden/g, '');
            } else if (bells[i].className.indexOf('crm-failed-order-hidden') === -1) {
                bells[i].className += ' crm-failed-order-hidden';
            }
            var badge = bells[i].querySelector('.crm-failed-order-bell-count');
            if (badge) {
                badge.textContent = count;
            }
        }
        ensureMenuBadge();
        var menus = document.querySelectorAll('#admin-navbar-side .crm-failed-order-badge');
        var menuLabel = menuBadgeLabel(total);
        for (i = 0; i < menus.length; i++) {
            menus[i].textContent = menuLabel;
            if (total > 999) {
                menus[i].setAttribute('title', '待处理 ' + count + ' 笔');
            } else if (menus[i].getAttribute('title')) {
                menus[i].removeAttribute('title');
            }
            if (total > 0) {
                menus[i].className = menus[i].className.replace(/\s*crm-failed-order-hidden/g, '');
            } else if (menus[i].className.indexOf('crm-failed-order-hidden') === -1) {
                menus[i].className += ' crm-failed-order-hidden';
            }
        }
    }

    function openFailedOrderTab() {
        var nodes = failedOrderAnchors();
        if (!nodes.length || !nodes[0].href) {
            if (layer) {
                layer.msg('当前账号没有审核失败订单菜单权限');
            }
            return false;
        }
        var meta = nodes[0];
        if (!tabApi || typeof tabApi.exists !== 'function' || typeof tabApi.tabAdd !== 'function') {
            if (layer) {
                layer.msg('暂时无法打开审核失败订单页面');
            }
            return false;
        }
        if (tabApi.exists(meta.title) !== -1) {
            var tabId = tabApi.getTabId(meta.title);
            if (element && tabId !== -1 && tabId !== undefined && tabId !== null) {
                element.tabChange('admin-tab', tabId);
                return true;
            }
        }
        var data = { href: meta.href, title: meta.title };
        var icon = meta.anchor.querySelector('i');
        var iconName = icon ? (icon.getAttribute('data-icon') || '') : '';
        if (iconName) {
            data.icon = iconName;
        }
        tabApi.tabAdd(data);
        return tabApi.exists(meta.title) !== -1;
    }

    function timeText(order) {
        if (!order || order.reject_time_known === false || order.reject_time_known === 0) {
            return '未知';
        }
        var text = order.reject_time_text == null ? '' : String(order.reject_time_text);
        if (!text || text === '未知') {
            return '未知';
        }
        return text;
    }

    function showOverdue(order) {
        if (timeText(order) === '未知') {
            return false;
        }
        return order.is_overdue === true || order.is_overdue === 1;
    }

    function buildReminderHtml(payload) {
        var maxInner = 480;
        if (typeof window !== 'undefined' && window.innerHeight) {
            maxInner = Math.max(180, Math.min(480, window.innerHeight - 180));
        }
        var html = '<div style="padding:16px 18px 8px;overflow:auto;max-height:' + maxInner + 'px;box-sizing:border-box;">';
        html += '<p style="margin:0 0 12px;line-height:1.7;font-size:14px;">您有 <strong>' + payload.total +
            '</strong> 笔审核失败订单需要处理，其中 <strong>' + payload.overdueTotal +
            '</strong> 笔已经超过 24 小时。</p>';
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px;">';
        html += '<thead><tr>' +
            '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">订单编号</th>' +
            '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">最近拒绝时间</th>' +
            '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">审核状态</th>' +
            '</tr></thead><tbody>';
        var orders = payload.orders || [];
        var i;
        for (i = 0; i < orders.length; i++) {
            var order = orders[i] || {};
            var overdue = showOverdue(order)
                ? ' <span style="color:#ff5722;font-weight:600;">超时未处理</span>'
                : '';
            var reason = order.reject_reason == null || String(order.reject_reason) === ''
                ? '无'
                : String(order.reject_reason);
            html += '<tr>' +
                '<td style="padding:8px;border-bottom:1px solid #f2f2f2;vertical-align:top;">' + escapeHtml(order.order_no || '') + '</td>' +
                '<td style="padding:8px;border-bottom:1px solid #f2f2f2;vertical-align:top;">' + escapeHtml(timeText(order)) + '</td>' +
                '<td style="padding:8px;border-bottom:1px solid #f2f2f2;vertical-align:top;">审核失败' + overdue + '</td>' +
                '</tr>';
            html += '<tr><td colspan="3" style="padding:0 8px 8px;color:#666;white-space:pre-wrap;word-break:break-word;">最近拒绝原因：' +
                escapeHtml(reason) + '</td></tr>';
        }
        html += '</tbody></table>';
        if (payload.total > orders.length) {
            html += '<p style="margin:12px 0 0;color:#ff5722;">还有 ' + (payload.total - orders.length) +
                ' 笔订单未展示，请前往审核失败订单页面查看。</p>';
        }
        html += '</div>';
        return html;
    }

    function popupWidth() {
        var vw = (typeof window !== 'undefined' && window.innerWidth) ? window.innerWidth : 1024;
        if (vw >= 800) {
            return 700;
        }
        return Math.max(300, vw - 24);
    }

    function commitShown(events) {
        var now = Date.now();
        var meta = readMeta();
        meta.lastShownAt = now;
        meta.nextAllowedAt = now + REPEAT_MS;
        if (lockToken) {
            meta.lock = { owner: TAB_ID, token: lockToken, until: now + LOCK_MS };
        }
        meta = rememberNonce(meta);
        writeMeta(meta);
        writeSnap(events);
        pendingShow = false;
    }

    function markLayer(layero) {
        if (!layero) {
            return;
        }
        if (typeof layero.attr === 'function') {
            layero.attr('data-crm-failed-reminder', '1');
            return;
        }
        if (layero.setAttribute) {
            layero.setAttribute('data-crm-failed-reminder', '1');
        }
    }

    function openReminder(payload) {
        if (popupOpen || !layer) {
            return false;
        }
        popupOpen = true;
        try {
            reminderLayerIndex = layer.open({
                type: 1,
                title: '审核失败订单待处理',
                area: [popupWidth() + 'px', 'auto'],
                shade: 0.3,
                shadeClose: false,
                closeBtn: 1,
                resize: false,
                content: buildReminderHtml(payload),
                btn: ['立即前往处理', '稍后处理'],
                yes: function (index) {
                    layer.close(index);
                    openFailedOrderTab();
                },
                success: function (layero) {
                    if (stopped) {
                        return;
                    }
                    markLayer(layero);
                    commitShown(payload.events);
                    startLockRenew();
                },
                end: function () {
                    popupOpen = false;
                    reminderLayerIndex = null;
                    stopLockRenew();
                    releaseLock();
                    if (remindAgainAfterClose) {
                        remindAgainAfterClose = false;
                        schedule(300);
                    }
                }
            });
        } catch (e) {
            popupOpen = false;
            reminderLayerIndex = null;
            return false;
        }
        if (reminderLayerIndex === undefined || reminderLayerIndex === null || reminderLayerIndex === false) {
            popupOpen = false;
            return false;
        }
        return true;
    }

    function applyDecisionSideEffects(decision) {
        if (decision.retry) {
            pendingShow = true;
            armDeferred();
        } else if (decision.newIds && decision.newIds.length && popupOpen) {
            remindAgainAfterClose = true;
        } else if (decision.action !== 'show') {
            pendingShow = false;
        }
        if (decision.writeSnapshot && lastPayload) {
            writeSnap(lastPayload.events);
        }
        if (decision.consumeFresh) {
            var meta = rememberNonce(readMeta());
            writeMeta(meta);
        }
    }

    function applySuccess(payload) {
        if (stopped) {
            return;
        }
        lastPayload = payload;
        updateBadges(payload.total);
        var meta = readMeta();
        var snap = readSnap();
        var decision = decideReminder({
            prevSnapshot: snap.events || {},
            currentSnapshot: payload.events,
            total: payload.total,
            now: Date.now(),
            nextAllowedAt: meta.nextAllowedAt || 0,
            freshLogin: computeFresh(meta),
            busy: isEditingBusy(),
            popupOpen: popupOpen,
            lockHeldByOther: false,
            hasBaseline: snap.events !== null
        });

        if (decision.action === 'show') {
            if (!tryAcquireLock()) {
                var metaAgain = readMeta();
                var snapAgain = readSnap();
                decision = decideReminder({
                    prevSnapshot: snapAgain.events || {},
                    currentSnapshot: payload.events,
                    total: payload.total,
                    now: Date.now(),
                    nextAllowedAt: metaAgain.nextAllowedAt || 0,
                    freshLogin: computeFresh(metaAgain),
                    busy: false,
                    popupOpen: popupOpen,
                    lockHeldByOther: true,
                    hasBaseline: snapAgain.events !== null
                });
            } else if (!openReminder(payload)) {
                releaseLock();
                decision = {
                    action: 'defer',
                    reason: decision.reason,
                    newIds: decision.newIds,
                    writeSnapshot: decision.reason !== 'new',
                    consumeFresh: false,
                    retry: true
                };
            } else {
                return;
            }
        }

        applyDecisionSideEffects(decision);
    }

    function parsePayload(data) {
        if (!data || typeof data !== 'object' || Array.isArray(data)) {
            return null;
        }
        if (typeof data.total !== 'number' || !isFinite(data.total) || data.total < 0 || Math.floor(data.total) !== data.total) {
            return null;
        }
        if (typeof data.overdue_total !== 'number' || !isFinite(data.overdue_total) || data.overdue_total < 0 || Math.floor(data.overdue_total) !== data.overdue_total) {
            return null;
        }
        if (!Array.isArray(data.orders)) {
            return null;
        }
        var events = normalizeEvents(data.active_reject_events);
        if (events === null) {
            return null;
        }
        return {
            total: data.total,
            overdueTotal: data.overdue_total,
            orders: data.orders.slice(0, 20),
            events: events
        };
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

    function parseResponse(text) {
        var raw = trimText(text);
        if (!raw || raw.charAt(0) === '<') {
            return { ok: false, authHtml: isLoginDocument(raw) };
        }
        var json;
        try {
            json = JSON.parse(raw);
        } catch (e) {
            return { ok: false, authHtml: false };
        }
        if (!json || typeof json !== 'object' || Array.isArray(json)) {
            return { ok: false, authHtml: false };
        }
        if (json.code !== 0 && json.code !== '0') {
            return { ok: false, authHtml: false };
        }
        var data = json.data;
        var remoteId = strictPositiveInt(data && typeof data === 'object' && !Array.isArray(data) ? data.admin_id : null);
        if (remoteId === null || pageAdminId < 1 || remoteId !== pageAdminId) {
            return { ok: false, authHtml: false, identityFailed: true };
        }
        var payload = parsePayload(data);
        if (!payload) {
            return { ok: false, authHtml: false };
        }
        return { ok: true, payload: payload, adminId: remoteId };
    }

    function closeOwnReminder() {
        var index = reminderLayerIndex;
        if (index === null || index === undefined) {
            popupOpen = false;
            return;
        }
        try {
            if (layer && typeof layer.close === 'function') {
                layer.close(index);
            }
        } catch (ignoreClose) {}
        popupOpen = false;
        reminderLayerIndex = null;
        stopLockRenew();
    }

    function stopForIdentityMismatch() {
        pollGeneration += 1;
        stopped = true;
        pendingShow = false;
        remindAgainAfterClose = false;
        refreshQueued = false;
        lastPayload = null;
        if (pollTimer) {
            clearTimeout(pollTimer);
            pollTimer = null;
        }
        if (deferredTimer) {
            clearTimeout(deferredTimer);
            deferredTimer = null;
        }
        stopLockRenew();
        closeOwnReminder();
        releaseLock();
        if (identityNoticeShown) {
            return;
        }
        identityNoticeShown = true;
        try {
            if (layer && typeof layer.msg === 'function') {
                layer.msg(IDENTITY_NOTICE);
            }
        } catch (ignoreNotice) {}
    }

    function onFail(authHtml) {
        failStreak += 1;
        if (authHtml && failStreak < 3) {
            failStreak = 3;
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
        if (stopped || isPolling) {
            return;
        }
        if (typeof document !== 'undefined' && document.hidden) {
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
                if (!parsed.ok) {
                    onFail(parsed.authHtml);
                    return;
                }
                failStreak = 0;
                applySuccess(parsed.payload);
            },
            error: function (xhr) {
                if (stopped || generation !== pollGeneration) {
                    return;
                }
                var authHtml = false;
                try {
                    authHtml = isLoginDocument(xhr && xhr.responseText);
                } catch (e) {}
                onFail(authHtml);
            },
            complete: function () {
                isPolling = false;
                if (stopped || generation !== pollGeneration || (typeof document !== 'undefined' && document.hidden)) {
                    return;
                }
                var immediate = refreshQueued;
                refreshQueued = false;
                schedule(immediate ? 0 : nextDelay());
            }
        });
    }

    function armDeferred() {
        if (deferredTimer || stopped) {
            return;
        }
        var generation = pollGeneration;
        deferredTimer = setTimeout(function () {
            deferredTimer = null;
            if (stopped || generation !== pollGeneration) {
                return;
            }
            if (typeof document !== 'undefined' && document.hidden) {
                return;
            }
            if (popupOpen) {
                if (pendingShow) {
                    armDeferred();
                }
                return;
            }
            if (!pendingShow || !lastPayload) {
                return;
            }
            if (isEditingBusy()) {
                armDeferred();
                return;
            }
            applySuccess(lastPayload);
        }, 3000);
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
        if (pendingShow) {
            armDeferred();
        }
        if (!isPolling) {
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
        if (deferredTimer) {
            clearTimeout(deferredTimer);
            deferredTimer = null;
        }
        stopLockRenew();
        releaseLock();
    }

    function resumeTimers() {
        if (stopped || isPolling || (listenersReadyAt && Date.now() - listenersReadyAt < 1500)) {
            return;
        }
        if (typeof document !== 'undefined' && document.hidden) {
            return;
        }
        if (pendingShow) {
            armDeferred();
        }
        tick();
    }

    function refreshNow() {
        if (stopped) {
            return;
        }
        refreshQueued = true;
        if (typeof document !== 'undefined' && document.hidden) {
            return;
        }
        if (isPolling) {
            return;
        }
        if (pollTimer) {
            clearTimeout(pollTimer);
            pollTimer = null;
        }
        tick();
    }

    function bindUi() {
        $(document)
            .off('click.crmFailedReminder', '.crm-failed-order-bell > a')
            .on('click.crmFailedReminder', '.crm-failed-order-bell > a', function (event) {
                event.preventDefault();
                openFailedOrderTab();
            });
    }

    function start(options) {
        if (started) {
            return;
        }
        options = options || {};
        var adminId = strictPositiveInt(options.adminId);
        if (adminId === null) {
            return;
        }
        if (!options.url || !options.jquery || !options.layer) {
            return;
        }
        $ = options.jquery;
        layer = options.layer;
        element = options.element;
        tabApi = options.tabApi;
        requestUrl = String(options.url);
        pageAdminId = adminId;
        metaKey = 'crm_for_v1_' + adminId + '_meta';
        snapKey = 'crm_for_v1_' + adminId + '_snap';
        started = true;
        if (typeof window !== 'undefined') {
            window.crmRefreshFailedOrderReminder = refreshNow;
        }
        bindUi();
        ensureMenuBadge();
        if (typeof document !== 'undefined') {
            document.addEventListener('visibilitychange', onVisibility);
        }
        if (typeof window !== 'undefined') {
            window.addEventListener('pagehide', pauseTimers);
            window.addEventListener('pageshow', resumeTimers);
        }
        listenersReadyAt = Date.now();
        tick();
    }

    return {
        start: start,
        escapeHtml: escapeHtml,
        normalizeEvents: normalizeEvents,
        diffNewEvents: diffNewEvents,
        decideReminder: decideReminder,
        isNewerReject: isNewerReject,
        eventKind: eventKind,
        parseResponse: parseResponse,
        strictPositiveInt: strictPositiveInt,
        showOverdue: showOverdue,
        timeText: timeText,
        buildReminderHtml: buildReminderHtml
    };
});
