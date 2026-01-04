-- ============================================================
-- 订单快照改造：为 crm_order_item 表添加供应商字段（如果字段不存在）
-- 说明：根据现有 SQL 文件，crm_order_item 表已经存在 supplier_id 和 supplier_name 字段
-- 如果您的数据库表结构中没有这两个字段，请执行以下 SQL
-- 注意：执行前请先检查字段是否已存在，避免重复执行
-- ============================================================

-- 检查字段是否存在（如果字段已存在，以下 SQL 会报错，可忽略）
-- 如果字段不存在，执行以下 ALTER TABLE 语句：

ALTER TABLE `crm_order_item` 
ADD COLUMN `supplier_id` varchar(255) DEFAULT NULL COMMENT '供应商ID（可选）' AFTER `spec_model`,
ADD COLUMN `supplier_name` varchar(200) DEFAULT NULL COMMENT '供应商名称（冗余保存）' AFTER `supplier_id`;

-- 说明：
-- 1. supplier_id 存储产品分类ID（category_id），用于快照保存供应商ID
-- 2. supplier_name 存储产品分类名称（category_name），用于快照保存供应商名称
-- 3. 这两个字段在订单添加/编辑时会自动从 crm_products 和 crm_product_category 表关联查询并快照保存
-- 4. 执行此 SQL 后，新增和编辑订单时会自动写入供应商信息快照

