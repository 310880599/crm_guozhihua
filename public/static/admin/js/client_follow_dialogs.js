/**
 * CRM 客户跟进弹窗（我的客户 / 我的订单等共用）
 * 依赖：jQuery、Layui 2.4.5（table、layer、laydate）
 * ES5，无构建步骤
 */
(function (window) {
    'use strict';

    var $ = window.jQuery || window.$;

    function bindJQuery() {
        if (!$ && typeof layui !== 'undefined' && layui.$) {
            $ = layui.$;
        }
        return $;
    }

    bindJQuery();

    var NS = '.clientFollowUI';
    var NEXT_UP_TIME_DEFAULT_CLOCK = '09:00:00';

    var state = {
        urls: {},
        onFollowSaved: null,
        enableRankEdit: true,
        enableDelete: true,
        followIndex: null,
        allFollowIndex: null,
        afTableIns: null,
        currentLeadsId: null,
        followSubmitting: false,
        followSubmitDefaultText: '保存跟进',
        nextUpLaydateInited: false,
        resizeTimer: null
    };

    function getLayer() {
        return typeof layui !== 'undefined' ? layui.layer : null;
    }

    function getTable() {
        return typeof layui !== 'undefined' ? layui.table : null;
    }

    function getLaydate() {
        return typeof layui !== 'undefined' ? layui.laydate : null;
    }

    function escapeHtml(text) {
        var s = text == null ? '' : String(text);
        return s
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatFollowRoleLabel(username, roleText) {
        var u = escapeHtml(username || '');
        var r = $.trim(roleText || '');
        if (!r) {
            return u;
        }
        return u + '\u3010' + escapeHtml(r) + '\u3011';
    }

    function normalizeNextUpTimeDisplay(value, emptyText) {
        var fallback = emptyText == null ? '' : String(emptyText);
        if (value == null) {
            return fallback;
        }
        var text = String(value).trim();
        if (text === '' || text === 'null' || text === 'undefined') {
            return fallback;
        }
        return text;
    }

    function normalizeNextUpTimeInput(value) {
        var text = $.trim(value || '');
        if (text === '') {
            return '';
        }
        var dateOnlyMatch = text.match(/^(\d{4}-\d{2}-\d{2})$/);
        if (dateOnlyMatch) {
            return dateOnlyMatch[1] + ' ' + NEXT_UP_TIME_DEFAULT_CLOCK;
        }
        var dateTimeMatch = text.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
        if (dateTimeMatch && dateTimeMatch[2] === '00' && dateTimeMatch[3] === '00' && dateTimeMatch[4] === '00') {
            return dateTimeMatch[1] + ' ' + NEXT_UP_TIME_DEFAULT_CLOCK;
        }
        return text;
    }

    function shouldApplyNextUpDefaultClock(value) {
        var text = $.trim(value || '');
        if (text === '') {
            return true;
        }
        if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
            return true;
        }
        return /^\d{4}-\d{2}-\d{2}\s+00:00:00$/.test(text);
    }

    function applyNextUpTimeDefault(value) {
        var normalized = normalizeNextUpTimeInput(value);
        if ($.trim($('#next_up_time').val()) !== normalized) {
            $('#next_up_time').val(normalized);
        }
        return normalized;
    }

    function syncNextUpTimeAfterLaydateDone(doneValue) {
        setTimeout(function () {
            var finalValue = $.trim($('#next_up_time').val());
            if (finalValue === '') {
                finalValue = $.trim(doneValue || '');
            }
            applyNextUpTimeDefault(finalValue);
        }, 0);
    }

    function initFollowNextUpTimeDefault(rawValue) {
        var text = $.trim(rawValue || '');
        if (text === '') {
            $('#next_up_time').val('');
            return '';
        }
        return applyNextUpTimeDefault(text);
    }

    function applyLaydatePanelDefaultClockIfNeeded(value) {
        if (!shouldApplyNextUpDefaultClock(value)) {
            return;
        }
        var parts = NEXT_UP_TIME_DEFAULT_CLOCK.split(':');
        if (parts.length !== 3) {
            return;
        }
        setTimeout(function () {
            var $panel = $('.layui-laydate:visible').last();
            if (!$panel.length) {
                return;
            }
            var $timeInputs = $panel.find('.laydate-time-text input');
            if ($timeInputs.length >= 3) {
                $timeInputs.eq(0).val(parts[0]).trigger('keyup').trigger('blur');
                $timeInputs.eq(1).val(parts[1]).trigger('keyup').trigger('blur');
                $timeInputs.eq(2).val(parts[2]).trigger('keyup').trigger('blur');
            }
            var $timeCols = $panel.find('.laydate-time-list ol');
            if (!$timeCols.length) {
                $timeCols = $panel.find('.laydate-time-list li ol');
            }
            if ($timeCols.length >= 3) {
                $.each(parts, function (idx, part) {
                    var $col = $timeCols.eq(idx);
                    var $target = $col.children('li').filter(function () {
                        return $.trim($(this).text()) === part;
                    }).first();
                    if ($target.length) {
                        $target.trigger('click');
                    }
                });
            }
        }, 0);
    }

    function sanitizeAvatarSrc(src) {
        var val = $.trim(String(src || ''));
        if (!val) {
            return '/static/admin/images/0.jpg';
        }
        if (/^(https?:)?\/\//i.test(val) || /^\//.test(val)) {
            return val.replace(/"/g, '%22').replace(/'/g, '%27');
        }
        return '/static/admin/images/0.jpg';
    }

    function padFollowTimeNum(num) {
        var s = String(num);
        return s.length < 2 ? '0' + s : s;
    }

    function parseFollowHistoryTimestamp(timeText) {
        var text = $.trim(String(timeText || ''));
        if (!text) {
            return 0;
        }
        if (/^\d{10,13}$/.test(text)) {
            var num = parseInt(text, 10);
            if (text.length === 13) {
                return num;
            }
            return num * 1000;
        }
        var ts = Date.parse(text.replace(/-/g, '/'));
        return isNaN(ts) ? 0 : ts;
    }

    function formatFollowHistoryTime(rawTime) {
        var text = $.trim(String(rawTime == null ? '' : rawTime));
        if (!text) {
            return '';
        }
        var ts = parseFollowHistoryTimestamp(text);
        if (!ts && /年|月|日/.test(text)) {
            var normalizedText = text.replace(/年/g, '-').replace(/月/g, '-').replace(/日/g, ' ').replace(/\s+/g, ' ').trim();
            ts = parseFollowHistoryTimestamp(normalizedText);
        }
        if (!ts) {
            return text;
        }
        var d = new Date(ts);
        if (isNaN(d.getTime())) {
            return text;
        }
        return d.getFullYear() + '-' + padFollowTimeNum(d.getMonth() + 1) + '-' + padFollowTimeNum(d.getDate()) + ' ' +
            padFollowTimeNum(d.getHours()) + ':' + padFollowTimeNum(d.getMinutes()) + ':' + padFollowTimeNum(d.getSeconds());
    }

    function normalizeFollowHistoryRecords(records) {
        var list = [];
        $.each(records || [], function (_, item) {
            item = item || {};
            var timeVal = item.time || item.create_date || item.created_at || item.create_time || item.add_time || '';
            var roleText = item.role_text || item.follow_role_text || '';
            list.push({
                time: formatFollowHistoryTime(timeVal),
                user: item.user || item.username || item.user_name || item.realname || item.oper_user || item.create_user || '系统',
                role_text: roleText,
                content: item.content || item.reply_msg || item.comment || '',
                avatar: item.avatar || ''
            });
        });
        list.sort(function (a, b) {
            return parseFollowHistoryTimestamp(b.time) - parseFollowHistoryTimestamp(a.time);
        });
        return list;
    }

    function ensureFollowMask() {
        var $mask = $('#follow-mask');
        if (!$mask.length) {
            $('body').append('<div id="follow-mask" class="follow-mask" style="display:none;"></div>');
            $mask = $('#follow-mask');
        }
        return $mask;
    }

    function showFollowMaskUnderLayer(layero) {
        var $mask = ensureFollowMask();
        var z = parseInt($(layero).css('z-index'), 10);
        if (!isNaN(z)) {
            $mask.css('z-index', z - 1);
        } else {
            $mask.css('z-index', 19999999);
        }
        $mask.show();
    }

    function hideFollowMask() {
        $('#follow-mask').hide();
    }

    function syncAllFollowTableRowHeights() {
        try {
            var $view = $('#all-follow-dialog #af-table').next('.layui-table-view');
            if (!$view.length) {
                return;
            }

            var $mainRows = $view.find('.layui-table-main tbody tr');
            if (!$mainRows.length) {
                return;
            }

            var $fixRRows = $view.find('.layui-table-fixed-r tbody tr');
            var $fixLRows = $view.find('.layui-table-fixed-l tbody tr');

            function setHImportant(el, h) {
                if (!el) {
                    return;
                }
                el.style.setProperty('height', h + 'px', 'important');
            }

            function resetHImportant(el) {
                if (!el) {
                    return;
                }
                el.style.removeProperty('height');
            }

            $mainRows.each(function () {
                resetHImportant(this);
                $(this).find('td').each(function () { resetHImportant(this); });
                $(this).find('td .layui-table-cell').each(function () { resetHImportant(this); });
            });

            if ($fixRRows.length) {
                $fixRRows.each(function () {
                    resetHImportant(this);
                    $(this).find('td').each(function () { resetHImportant(this); });
                    $(this).find('td .layui-table-cell').each(function () { resetHImportant(this); });
                });
            }

            if ($fixLRows.length) {
                $fixLRows.each(function () {
                    resetHImportant(this);
                    $(this).find('td').each(function () { resetHImportant(this); });
                    $(this).find('td .layui-table-cell').each(function () { resetHImportant(this); });
                });
            }

            $mainRows.each(function (index) {
                var mainRow = this;
                var fixRRow = ($fixRRows.length > index) ? $fixRRows.get(index) : null;
                var fixLRow = ($fixLRows.length > index) ? $fixLRows.get(index) : null;

                var hMain = $(mainRow).outerHeight() || 0;
                var hR = fixRRow ? ($(fixRRow).outerHeight() || 0) : 0;
                var hL = fixLRow ? ($(fixLRow).outerHeight() || 0) : 0;

                var h = Math.max(hMain, hR, hL);
                if (h <= 0) {
                    return;
                }

                setHImportant(mainRow, h);
                $(mainRow).find('td').each(function () { setHImportant(this, h); });
                $(mainRow).find('td .layui-table-cell').each(function () { setHImportant(this, h); });

                if (fixRRow) {
                    setHImportant(fixRRow, h);
                    $(fixRRow).find('td').each(function () { setHImportant(this, h); });
                    $(fixRRow).find('td .layui-table-cell').each(function () { setHImportant(this, h); });
                }

                if (fixLRow) {
                    setHImportant(fixLRow, h);
                    $(fixLRow).find('td').each(function () { setHImportant(this, h); });
                    $(fixLRow).find('td .layui-table-cell').each(function () { setHImportant(this, h); });
                }
            });
        } catch (e) {
            if (window.console && console.warn) {
                console.warn('全部跟进表格行高同步失败:', e);
            }
        }
    }

    function scheduleSyncAllFollowTableRowHeights() {
        syncAllFollowTableRowHeights();
        setTimeout(syncAllFollowTableRowHeights, 0);
        setTimeout(syncAllFollowTableRowHeights, 50);
    }

    function bindAllFollowTableScrollSync() {
        var $view = $('#all-follow-dialog #af-table').next('.layui-table-view');
        if (!$view.length) {
            return;
        }
        var scrollTimer = null;
        $view.find('.layui-table-main, .layui-table-fixed-r .layui-table-body, .layui-table-fixed-l .layui-table-body')
            .off('scroll' + NS)
            .on('scroll' + NS, function () {
                if (scrollTimer) {
                    clearTimeout(scrollTimer);
                }
                scrollTimer = setTimeout(function () {
                    syncAllFollowTableRowHeights();
                }, 100);
            });
    }

    function parseAfDateRange() {
        var dateRange = $.trim($('#af-date-range').val());
        var startDate = '';
        var endDate = '';
        if (dateRange) {
            var parts = dateRange.split(' - ');
            if (parts.length === 2) {
                startDate = $.trim(parts[0]);
                endDate = $.trim(parts[1]);
            }
        }
        return { startDate: startDate, endDate: endDate };
    }

    function getAfTableReloadWhere() {
        var dr = parseAfDateRange();
        return {
            leads_id: state.currentLeadsId,
            keyword: $.trim($('#af-keyword').val()),
            start_date: dr.startDate,
            end_date: dr.endDate
        };
    }

    function setFollowSubmitState(isSubmitting) {
        state.followSubmitting = !!isSubmitting;
        $('#submit-comment')
            .prop('disabled', state.followSubmitting)
            .toggleClass('layui-btn-disabled', state.followSubmitting)
            .text(state.followSubmitting ? '提交中...' : state.followSubmitDefaultText);
    }

    function updateFollowWritePanel(client) {
        client = client || {};
        var canWrite = client.can_write_follow !== false;
        var currentRole = $.trim(client.current_role || '');
        var showPanel = canWrite && currentRole !== '';
        if (showPanel) {
            $('#follow-write-panel').show();
            $('#follow-write-denied').hide();
        } else {
            $('#follow-write-panel').hide();
            $('#follow-write-denied').show();
        }
    }

    function renderFollowHistory(comments) {
        var normalized = normalizeFollowHistoryRecords(comments || []);
        var timelineHtml = '';
        if (normalized.length > 0) {
            $.each(normalized, function (_, item) {
                var avatar = sanitizeAvatarSrc(item.avatar);
                var userHtml = formatFollowRoleLabel(item.user || '系统', item.role_text || '');
                timelineHtml += '<div class="timeline-item">' +
                    '<div class="timeline-title">' + escapeHtml(item.time || '') + '</div>' +
                    '<div class="timeline-content">' +
                    '<img src="' + avatar + '" class="avatar" onerror="this.src=\'/static/admin/images/0.jpg\'">' +
                    '<strong>' + userHtml + '：</strong>' +
                    escapeHtml(item.content || '') +
                    '</div>' +
                    '</div>';
            });
        } else {
            timelineHtml = '<p>暂无跟进记录</p>';
        }
        $('#follow-history-list').html(timelineHtml);
        return timelineHtml;
    }

    function renderFollowDialogData(clientData, comments, options) {
        options = options || {};
        clientData = clientData || {};

        $('#follow-client-id').text(clientData.id || '');
        $('#follow-client-name').text(clientData.kh_name || '');
        $('#follow-contact').text(clientData.kh_contact || '');
        $('#follow-main-phone').text(clientData.main_phone || '暂无');
        $('#follow-aux-phone').text(clientData.aux_phone || '暂无');
        $('#follow-area').text(clientData.xs_area || '');
        $('#follow-source').text(clientData.inquiry_name || clientData.inquiry_id || '');
        $('#follow-source-port').text(clientData.port_name || clientData.port_id || '暂无');

        var rankDisplayText = clientData.kh_rank_name || clientData.kh_rank || '';
        $('#follow-rank').text(rankDisplayText);
        $('#follow-rank-display').show();
        $('#follow-rank-editor').hide();
        $('#follow-rank-row').data('clientId', clientData.id)
            .data('khRank', clientData.kh_rank || '')
            .data('rankDisplayText', rankDisplayText);

        if (state.enableRankEdit) {
            $('#follow-rank-edit-btn').show();
        } else {
            $('#follow-rank-edit-btn').hide();
        }

        $('#follow-user').text(clientData.pr_user || '');
        $('#follow-joint-person').text(clientData.joint_person_names || '暂无');
        $('#follow-current-operator').text(clientData.current_operator || '');
        $('#follow-current-role').text(clientData.current_role_text || '');
        $('#follow-create-time').text(clientData.at_time || '');
        $('#follow-last-time').text(clientData.last_up_time || '暂无');
        $('#follow-next-time').text(normalizeNextUpTimeDisplay(clientData.next_up_time, '--'));
        $('#follow-last-record').val(clientData.last_up_records || '暂无记录');
        $('#leads_id').val(clientData.id || '');

        updateFollowWritePanel(clientData);

        if (options.resetForm !== false) {
            $('#reply_msg').val('');
            initFollowNextUpTimeDefault(clientData.next_up_time || '');
        }

        renderFollowHistory(comments || []);
    }

    function requestFollowDialogData(clientId, options) {
        options = options || {};
        var layer = getLayer();
        var showLoading = options.showLoading !== false;
        var loading = null;

        if (showLoading && layer) {
            loading = layer.load(1, { shade: 0 });
        }

        $.ajax({
            url: state.urls.detail,
            method: 'GET',
            data: { id: clientId },
            success: function (res) {
                if (loading !== null && layer) {
                    layer.close(loading);
                }
                if (res.code !== 0 || !res.data || !res.data.client) {
                    if (typeof options.onError === 'function') {
                        options.onError(res.msg || '获取客户信息失败');
                    } else if (layer) {
                        layer.msg(res.msg || '获取客户信息失败', { icon: 2 });
                    }
                    return;
                }
                if (typeof options.onSuccess === 'function') {
                    options.onSuccess(res.data);
                }
            },
            error: function () {
                if (loading !== null && layer) {
                    layer.close(loading);
                }
                if (typeof options.onError === 'function') {
                    options.onError('网络错误，获取客户信息失败');
                } else if (layer) {
                    layer.msg('网络错误，获取客户信息失败', { icon: 2 });
                }
            }
        });
    }

    function initAllFollowTable(leadsId) {
        var table = getTable();
        if (!table) {
            return;
        }

        setTimeout(function () {
            if (state.afTableIns) {
                try {
                    var $table = $('#af-table');
                    if ($table.length && $table.next('.layui-table-view').length) {
                        $table.next('.layui-table-view').remove();
                    }
                } catch (e) {
                    if (window.console && console.warn) {
                        console.warn('销毁旧表格失败:', e);
                    }
                }
                state.afTableIns = null;
            }

            var allFollowCols = [
                { field: 'create_date', title: '跟进时间', width: 180, sort: true },
                {
                    field: 'username',
                    title: '跟进人',
                    width: 240,
                    templet: function (d) {
                        var name = d.username == null ? '' : String(d.username);
                        var roleText = d.follow_role_text == null ? '' : String(d.follow_role_text);
                        return formatFollowRoleLabel(name, roleText);
                    }
                },
                {
                    field: 'reply_msg',
                    title: '跟进内容',
                    minWidth: 420,
                    templet: function (d) {
                        var txt = d.reply_msg == null ? '' : String(d.reply_msg);
                        var esc = escapeHtml(txt);
                        return '<div class="af-reply-msg-cell">' + esc + '</div>';
                    }
                }
            ];

            if (state.enableDelete !== false) {
                allFollowCols.push({
                    field: 'operate',
                    title: '操作',
                    width: 100,
                    templet: function (d) {
                        return '<button type="button" class="layui-btn layui-btn-danger layui-btn-xs delete-follow-btn" data-id="' + escapeHtml(d.id) + '">删除</button>';
                    }
                });
            }

            state.afTableIns = table.render({
                elem: '#af-table',
                id: 'af-table',
                url: state.urls.commentsPage,
                method: 'post',
                page: true,
                limit: 10,
                limits: [5, 10, 20, 50, 100, 200, 500],
                cols: [allFollowCols],
                where: {
                    leads_id: leadsId,
                    keyword: '',
                    start_date: '',
                    end_date: ''
                },
                done: function () {
                    scheduleSyncAllFollowTableRowHeights();
                    bindAllFollowTableScrollSync();
                }
            });
        }, 100);
    }

    function reloadAfTable(extra) {
        var table = getTable();
        if (!table || !state.afTableIns || !state.currentLeadsId) {
            return;
        }
        var opts = {
            where: getAfTableReloadWhere(),
            done: function () {
                scheduleSyncAllFollowTableRowHeights();
            }
        };
        if (extra && extra.page) {
            opts.page = extra.page;
        }
        table.reload('af-table', opts);
    }

    function initNextUpTimeLaydate() {
        if (state.nextUpLaydateInited) {
            return;
        }
        var laydate = getLaydate();
        if (!laydate || !$('#next_up_time').length) {
            return;
        }
        state.nextUpLaydateInited = true;
        laydate.render({
            elem: '#next_up_time',
            type: 'datetime',
            format: 'yyyy-MM-dd HH:mm:ss',
            trigger: 'click',
            ready: function () {
                applyLaydatePanelDefaultClockIfNeeded($.trim($('#next_up_time').val()));
            },
            change: function (value) {
                if (value) {
                    applyNextUpTimeDefault(value);
                }
                applyLaydatePanelDefaultClockIfNeeded(value);
            },
            done: function (value) {
                syncNextUpTimeAfterLaydateDone(value);
            }
        });
    }

    function unbindAllEvents() {
        $(document).off(NS);
        $('#submit-comment').off(NS);
        $('#af-search-btn').off(NS);
        $('#af-reset-btn').off(NS);
        $('#follow-rank-edit-btn').off(NS);
        $('#follow-rank-cancel-btn').off(NS);
        $('#follow-rank-save-btn').off(NS);
        $('#next_up_time').off(NS);
        $(window).off('resize' + NS);
    }

    function bindEvents() {
        unbindAllEvents();

        $('#submit-comment').on('click' + NS, function (e) {
            e.preventDefault();
            e.stopPropagation();
            var layer = getLayer();

            if (state.followSubmitting) {
                return;
            }

            var replyMsg = $.trim($('#reply_msg').val());
            var nextUpTime = applyNextUpTimeDefault($.trim($('#next_up_time').val()));
            var leadsId = $('#leads_id').val();

            if (!replyMsg) {
                if (layer) {
                    layer.msg('请输入跟进内容', { icon: 0 });
                }
                return;
            }
            if (!leadsId) {
                if (layer) {
                    layer.msg('缺少客户ID，请关闭弹窗后重试', { icon: 2 });
                }
                return;
            }

            setFollowSubmitState(true);

            $.ajax({
                method: 'post',
                url: state.urls.save,
                data: {
                    reply_msg: replyMsg,
                    leads_id: leadsId,
                    next_up_time: nextUpTime
                },
                success: function (res) {
                    if (res.code === 0) {
                        if (layer) {
                            layer.msg(res.msg || '保存成功', { icon: 1 });
                        }
                        $('#reply_msg').val('');
                        initFollowNextUpTimeDefault('');

                        requestFollowDialogData(leadsId, {
                            showLoading: false,
                            onSuccess: function (data) {
                                renderFollowDialogData(data.client || {}, data.comments || [], { resetForm: true });
                            },
                            onError: function (msg) {
                                if (layer) {
                                    layer.msg('保存成功，但刷新跟进信息失败：' + msg, { icon: 0, time: 2500 });
                                }
                            }
                        });

                        if (typeof state.onFollowSaved === 'function') {
                            state.onFollowSaved(leadsId, res);
                        }
                    } else if (layer) {
                        layer.msg(res.msg || '保存失败，请稍后重试', { icon: 2 });
                    }
                },
                error: function () {
                    if (layer) {
                        layer.msg('提交失败，请检查网络后重试', { icon: 2 });
                    }
                },
                complete: function () {
                    setFollowSubmitState(false);
                }
            });
        });

        $(document).on('click' + NS, '#af-search-btn', function () {
            var layer = getLayer();
            if (!state.afTableIns || !state.currentLeadsId) {
                if (layer) {
                    layer.msg('表格未初始化', { icon: 0 });
                }
                return;
            }
            reloadAfTable({ page: { curr: 1 } });
        });

        $(document).on('click' + NS, '#af-reset-btn', function () {
            var layer = getLayer();
            if (!state.afTableIns || !state.currentLeadsId) {
                if (layer) {
                    layer.msg('表格未初始化', { icon: 0 });
                }
                return;
            }
            $('#af-keyword').val('');
            $('#af-date-range').val('');
            reloadAfTable({ page: { curr: 1 } });
        });

        $(document).on('click' + NS, '.delete-follow-btn', function () {
            var layer = getLayer();
            var commentId = $(this).data('id');
            if (!commentId) {
                if (layer) {
                    layer.msg('参数错误', { icon: 2 });
                }
                return;
            }

            layer.confirm('确定删除这条跟进记录吗？删除后不可恢复！', {
                title: '信息',
                zIndex: layer.zIndex + 100,
                success: function (layero) {
                    layer.setTop(layero);
                }
            }, function (index) {
                $.post(state.urls.deleteComment, {
                    comment_id: commentId
                }, function (res) {
                    if (res.code === 0) {
                        layer.msg(res.msg || '删除成功', { icon: 1 });
                        reloadAfTable();
                    } else {
                        layer.msg(res.msg || '删除失败', { icon: 2 });
                    }
                });
                layer.close(index);
            });
        });

        if (state.enableRankEdit) {
            $(document).on('click' + NS, '#follow-rank-edit-btn', function () {
                var layer = getLayer();
                var $row = $('#follow-rank-row');
                var clientId = $row.data('clientId');
                if (!clientId) {
                    return;
                }

                var $select = $('#follow-rank-select');
                $select.empty().append('<option value="">加载中...</option>');
                $('#follow-rank-display').hide();
                $('#follow-rank-editor').show();

                $.ajax({
                    url: state.urls.rankOptions,
                    method: 'GET',
                    data: { id: clientId },
                    success: function (res) {
                        if (res.code === 0 && res.data) {
                            $select.empty().append('<option value="">请选择</option>');
                            var currentKhRank = $row.data('khRank') + '';
                            $.each(res.data, function (_, item) {
                                var display = item.rank_name_display || item.rank_name;
                                var selected = '';
                                if (currentKhRank === (item.id + '')) {
                                    selected = ' selected';
                                }
                                $select.append('<option value="' + item.id + '"' + selected + '>' + escapeHtml(display) + '</option>');
                            });
                        } else {
                            if (layer) {
                                layer.msg(res.msg || '获取级别选项失败');
                            }
                            $('#follow-rank-display').show();
                            $('#follow-rank-editor').hide();
                        }
                    },
                    error: function () {
                        if (layer) {
                            layer.msg('网络错误，获取级别选项失败');
                        }
                        $('#follow-rank-display').show();
                        $('#follow-rank-editor').hide();
                    }
                });
            });

            $(document).on('click' + NS, '#follow-rank-cancel-btn', function () {
                $('#follow-rank-editor').hide();
                $('#follow-rank-display').show();
            });

            $(document).on('click' + NS, '#follow-rank-save-btn', function () {
                var layer = getLayer();
                var $row = $('#follow-rank-row');
                var clientId = $row.data('clientId');
                var newRankId = $('#follow-rank-select').val();
                if (!newRankId) {
                    if (layer) {
                        layer.msg('请选择客户级别');
                    }
                    return;
                }

                var $btn = $(this);
                $btn.prop('disabled', true).text('保存中');

                $.ajax({
                    url: state.urls.updateRank,
                    method: 'POST',
                    data: { id: clientId, kh_rank: newRankId },
                    success: function (res) {
                        $btn.prop('disabled', false).text('保存');
                        if (res.code === 0 && res.data) {
                            if (layer) {
                                layer.msg('客户级别修改成功');
                            }
                            var newName = res.data.kh_rank_name || '';
                            $('#follow-rank').text(newName);
                            $row.data('khRank', res.data.kh_rank || '')
                                .data('rankDisplayText', newName);
                            $('#follow-rank-editor').hide();
                            $('#follow-rank-display').show();
                        } else if (layer) {
                            layer.msg(res.msg || '保存失败');
                        }
                    },
                    error: function () {
                        $btn.prop('disabled', false).text('保存');
                        if (layer) {
                            layer.msg('网络错误，保存失败');
                        }
                    }
                });
            });
        }

        $('#next_up_time').on('blur' + NS, function () {
            applyNextUpTimeDefault(this.value);
        });

        $(window).on('resize' + NS, function () {
            if (state.resizeTimer) {
                clearTimeout(state.resizeTimer);
            }
            state.resizeTimer = setTimeout(function () {
                syncAllFollowTableRowHeights();
            }, 150);
        });
    }

    function openFollowDialog(leadsId) {
        var layer = getLayer();
        if (!layer) {
            return;
        }

        requestFollowDialogData(leadsId, {
            showLoading: true,
            onSuccess: function (data) {
                var clientData = data.client || {};
                var comments = data.comments || [];
                renderFollowDialogData(clientData, comments, { resetForm: true });

                if (state.followIndex !== null) {
                    layer.close(state.followIndex);
                    state.followIndex = null;
                }

                state.followIndex = layer.open({
                    type: 1,
                    title: '客户跟进 - ' + (clientData.kh_name || ''),
                    area: ['90%', '80%'],
                    content: $('#follow-dialog'),
                    closeBtn: 1,
                    shade: 0,
                    shadeClose: false,
                    key: true,
                    anim: -1,
                    success: function (layero, index) {
                        $('#follow-dialog').show();
                        layer.setTop(layero);
                        showFollowMaskUnderLayer(layero);
                        $(layero).find('.layui-layer-close').off('click' + NS).on('click' + NS, function (e) {
                            e.preventDefault();
                            e.stopPropagation();
                            layer.close(index);
                        });
                    },
                    end: function () {
                        $('#follow-dialog').hide().appendTo('body');
                        hideFollowMask();
                        $('.layui-layer-shade').remove();
                        state.followIndex = null;
                        setFollowSubmitState(false);
                    }
                });
            }
        });
    }

    function openAllFollowDialog(leadsId) {
        var layer = getLayer();
        if (!layer) {
            return;
        }

        state.currentLeadsId = leadsId;
        var loading = layer.load(1, { shade: 0 });

        $.ajax({
            url: state.urls.detail,
            method: 'GET',
            data: { id: leadsId },
            success: function (res) {
                layer.close(loading);
                if (res.code !== 0 || !res.data || !res.data.client) {
                    layer.msg(res.msg || '获取客户信息失败');
                    return;
                }

                var clientData = res.data.client;
                $('#af-kh-name').text(clientData.kh_name || '');
                $('#af-product-name').text(clientData.product_name || '暂无');
                $('#af-main-phone').text(clientData.main_phone || '暂无');
                $('#af-aux-phone').text(clientData.aux_phone || '暂无');

                if (state.allFollowIndex !== null) {
                    layer.close(state.allFollowIndex);
                    state.allFollowIndex = null;
                }

                state.allFollowIndex = layer.open({
                    type: 1,
                    title: '全部跟进 - ' + (clientData.kh_name || ''),
                    area: ['90%', '80%'],
                    content: $('#all-follow-dialog'),
                    closeBtn: 1,
                    shade: 0,
                    shadeClose: false,
                    key: true,
                    anim: -1,
                    success: function (layero, index) {
                        $('#all-follow-dialog').show();
                        layer.setTop(layero);
                        showFollowMaskUnderLayer(layero);

                        $(layero).find('.layui-layer-close').off('click' + NS).on('click' + NS, function (e) {
                            e.preventDefault();
                            e.stopPropagation();
                            layer.close(index);
                        });

                        var laydate = getLaydate();
                        if (laydate) {
                            laydate.render({
                                elem: '#af-date-range',
                                type: 'date',
                                range: true,
                                trigger: 'click'
                            });
                        }

                        initAllFollowTable(leadsId);
                    },
                    end: function () {
                        $('#all-follow-dialog').hide().appendTo('body');
                        hideFollowMask();
                        $('.layui-layer-shade').remove();

                        $('#af-keyword').val('');
                        $('#af-date-range').val('');

                        if (state.afTableIns) {
                            try {
                                var $table = $('#af-table');
                                if ($table.length && $table.next('.layui-table-view').length) {
                                    $table.next('.layui-table-view').remove();
                                }
                            } catch (e) {
                                if (window.console && console.warn) {
                                    console.warn('清理表格失败:', e);
                                }
                            }
                            state.afTableIns = null;
                        }

                        var $view = $('#all-follow-dialog #af-table').next('.layui-table-view');
                        if ($view.length) {
                            $view.find('.layui-table-main, .layui-table-fixed-r .layui-table-body, .layui-table-fixed-l .layui-table-body')
                                .off('scroll' + NS);
                        }

                        state.allFollowIndex = null;
                        state.currentLeadsId = null;
                    }
                });
            },
            error: function () {
                layer.close(loading);
                layer.msg('网络错误，获取客户信息失败');
            }
        });
    }

    function init(options) {
        options = options || {};
        if (!bindJQuery()) {
            return;
        }

        state.urls = options.urls || {};
        state.onFollowSaved = typeof options.onFollowSaved === 'function' ? options.onFollowSaved : null;
        state.enableRankEdit = options.enableRankEdit !== false;
        state.enableDelete = options.enableDelete !== false;

        state.followSubmitDefaultText = $.trim($('#submit-comment').text()) || '保存跟进';

        initNextUpTimeLaydate();
        bindEvents();
    }

    window.ClientFollowUI = {
        init: init,
        openFollowDialog: openFollowDialog,
        openAllFollowDialog: openAllFollowDialog,
        formatFollowRoleLabel: formatFollowRoleLabel,
        escapeHtml: escapeHtml
    };

})(window);
