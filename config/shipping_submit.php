<?php
/**
 * 发货正式提交 HTTP 接口开关（服务端文件配置）
 *
 * 安全约定：
 * - 默认必须为 true
 * - 仅当值为严格布尔 true 时，Controller 才允许进入 Service
 * - 不得由请求参数、超管配置页或登录身份单独开启
 * - 未完成 CSRF 与隔离环境验收前，禁止改为 true
 */
return [
    'enabled' => true,
];
