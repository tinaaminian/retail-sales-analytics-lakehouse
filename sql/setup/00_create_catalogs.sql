CREATE CATALOG IF NOT EXISTS dev_shop COMMENT 'ShopStream - development';
CREATE CATALOG IF NOT EXISTS stg_shop COMMENT 'ShopStream - staging/ integration test';
CREATE CATALOG IF NOT EXISTS prd_shop COMMENT 'ShopStream - production';

ALTER CATALOG dev_shop SET TAGS ('env' = 'dev' , 'project'= 'shopstream');
ALTER CATALOG stg_shop SET TAGS ('env' = 'stg' , 'project' = 'shopstream');
ALTER CATALOG prd_shop SET TAGS ('env' = 'prd',  'project' = 'shopstream');