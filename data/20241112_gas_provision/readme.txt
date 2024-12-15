CREATE OR REPLACE TABLE `octan-infra.layerzero_interactors.20241114_first_transactions`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
SELECT 
  to_address,
  from_address AS provider_address,
  value AS amount_eth,
  block_timestamp AS timestamp,
  block_number,
  transaction_hash AS tx_hash
FROM (
  SELECT 
    to_address,
    from_address,
    value,
    block_timestamp,
    block_number,
    transaction_hash,
    ROW_NUMBER() OVER (PARTITION BY to_address ORDER BY block_timestamp, transaction_hash) AS rn,
    COUNT(*) OVER (PARTITION BY to_address, block_timestamp, block_number) AS tx_count_in_block
  FROM 
    `bigquery-public-data.crypto_ethereum.traces` t
 WHERE 
    value > 0 
)
WHERE rn = 1
  AND tx_count_in_block = 1;













CREATE OR REPLACE TABLE `octan-infra.layerzero_interactors.20241114_l0_interactor_provision_network`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS

WITH RECURSIVE gas_provisions AS (
  SELECT 
    to_address,
    t.provider_address AS provider_address,
    t.amount_eth AS amount_eth,
    t.timestamp AS timestamp,
    t.tx_hash AS tx_hash,
    t.block_number,
    1 AS level
  FROM 
    `octan-infra.layerzero_interactors.20241114_first_transactions` AS t
  LEFT JOIN
    `octan-infra.layerzero_interactors.cex_dex_labels` AS cex_dex
    ON t.provider_address = cex_dex.address
  WHERE 
    to_address IN (SELECT addr FROM `octan-infra.layerzero_interactors.layerzero_interactors`)

  UNION ALL

  SELECT 
    t.to_address,
    t.provider_address AS provider_address,
    t.amount_eth AS amount_eth,
    t.timestamp AS timestamp,
    t.tx_hash AS tx_hash,
    t.block_number,
    gp.level + 1 AS level
  FROM 
    `octan-infra.layerzero_interactors.20241114_first_transactions` AS t
  JOIN 
    gas_provisions AS gp 
    ON t.to_address = gp.provider_address
  WHERE 
     gp.level < 500
)

SELECT DISTINCT
  to_address as activated_address,
  provider_address as gas_provider,
  amount_eth as gas_provision_amount,
  timestamp as first_gas_provision_time,
  block_number,
  tx_hash
FROM gas_provisions
ORDER BY to_address, timestamp;

