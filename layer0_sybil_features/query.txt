WITH addresses AS (
  SELECT
    sender_wallet AS addr,
    -- Number of ETH - LayerZero interactions
    COUNT(DISTINCT source_transaction_hash) AS n_eth_interactions
  FROM
    external.layerzero.fact_transactions_snapshot
  WHERE
    source_chain = 'Ethereum'
  GROUP BY
    1
),
layerzero_data AS (
  SELECT
    a.addr,
    a.n_eth_interactions,
    COUNT(DISTINCT b.source_chain) AS n_l0_source_chains,
    COUNT(DISTINCT b.destination_chain) AS n_l0_dest_chains,
    COUNT(DISTINCT b.destination_chain) / COUNT(DISTINCT b.source_chain) AS n_l0_dest_chain_per_source_chain,
    COUNT(DISTINCT b.project) as n_l0_projects,
    COUNT(DISTINCT b.project) / COUNT(DISTINCT b.source_chain) AS n_l0_project_per_source_chain,
    COUNT(DISTINCT b.source_contract) as n_l0_source_contracts,
    COUNT(DISTINCT b.destination_contract) as n_l0_dest_contracts,
    MAX(COALESCE(b.stargate_swap_usd, 0)) AS l0_max_stargate_swap,
    AVG(COALESCE(b.stargate_swap_usd, 0)) AS l0_avg_stargate_swap,    
    MIN(COALESCE(b.stargate_swap_usd, 0)) AS l0_min_stargate_swap,
    MAX(COALESCE(b.native_drop_usd, 0)) AS l0_max_native_drop_usd,
    AVG(COALESCE(b.native_drop_usd, 0)) AS l0_avg_native_drop_usd,    
    COUNT(DISTINCT b.source_transaction_hash) AS n_l0_txs,
    MIN(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS earliest_l0_tx_time,
    MAX(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS latest_l0_tx_time,
    MAX(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) - MIN(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS l0_tx_time_span
  FROM
    addresses a
    JOIN external.layerzero.fact_transactions_snapshot b ON a.addr = b.sender_wallet
  GROUP BY
    1,
    2
),
layerzero_to_eth AS (
  SELECT
    a.addr,
    COUNT(DISTINCT b.source_chain) AS n_l0_to_eth_source_chains,
    COUNT(DISTINCT b.project) as n_l0_to_eth_projects,
    COUNT(DISTINCT b.project) / COUNT(DISTINCT b.source_chain) AS n_l0_to_eth_project_per_source_chain,
    COUNT(DISTINCT b.source_contract) as n_l0_to_eth_source_contracts,
    COUNT(DISTINCT b.destination_contract) as n_l0_to_eth_dest_contracts,
    MAX(COALESCE(b.stargate_swap_usd, 0)) AS l0_to_eth_max_stargate_swap,
    AVG(COALESCE(b.stargate_swap_usd, 0)) AS l0_to_eth_avg_stargate_swap,    
    MIN(COALESCE(b.stargate_swap_usd, 0)) AS l0_to_eth_min_stargate_swap,
    MAX(COALESCE(b.native_drop_usd, 0)) AS l0_to_eth_max_native_drop_usd,
    AVG(COALESCE(b.native_drop_usd, 0)) AS l0_to_eth_avg_native_drop_usd,    
    COUNT(DISTINCT b.source_transaction_hash) AS n_l0_to_eth_txs,
    MIN(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS earliest_l0_to_eth_tx_time,
    MAX(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS latest_l0_to_eth_tx_time,
    MAX(date_part(epoch_second,CAST(source_timestamp_utc AS timestamp with time zone))) - MIN(date_part(epoch_second, CAST(source_timestamp_utc AS timestamp with time zone))) AS l0_to_eth_tx_time_span
  FROM
    addresses a
    JOIN external.layerzero.fact_transactions_snapshot b ON a.addr = b.sender_wallet
    WHERE destination_chain = 'Ethereum'
  GROUP BY
    1
),
ethereum_tx_data AS (
  SELECT
    from_address AS addr,
    COUNT(*) AS num_transactions,
    MIN(block_number) AS earliest_tx_block_out,
    MAX(block_number) AS latest_tx_block_out,
    MAX(block_number) - MIN(block_number) AS tx_block_span,
    MAX(value) AS max_tx_value_out,
    MIN(value) AS min_tx_value_out,
    SUM(value) AS total_tx_value_out,
    AVG(value) AS avg_tx_value_out,
    MAX(tx_fee) AS max_tx_fee_out,
    MIN(tx_fee) AS min_tx_fee_out,
    COUNT(DISTINCT to_address) AS out_degree
  FROM
    ethereum.core.fact_transactions a
    JOIN addresses b on a.from_address = b.addr
  WHERE
    block_timestamp <= TO_TIMESTAMP_NTZ('2024-05-01 00:00:00')
  GROUP BY
    from_address
),
ethereum_in_tx_data AS (
  -- Inbound transaction metrics
  SELECT
    to_address AS addr,
    COUNT(*) AS num_transactions_in,
    MIN(block_number) AS earliest_tx_block_in,
    MAX(block_number) AS latest_tx_block_in,
    MAX(value) AS max_tx_value_in,
    MIN(value) AS min_tx_value_in,
    SUM(value) AS total_tx_value_in,
    AVG(value) AS avg_tx_value_in,
    COUNT(DISTINCT from_address) AS in_degree
  FROM
    ethereum.core.fact_transactions a
    JOIN addresses b ON a.to_address = b.addr
  WHERE
    block_timestamp < TO_TIMESTAMP_NTZ('2024-05-01 00:00:00')
  GROUP BY
    to_address
)
SELECT
  l.*,
  leth.* EXCLUDE (addr),
    e.* EXCLUDE (addr),
  e.latest_tx_block_out - e.earliest_tx_block_out AS time_span_out,
  CASE
    WHEN (COALESCE(e.latest_tx_block_out - e.earliest_tx_block_out,0)) = 0 THEN 0
    ELSE e.out_degree / ((e.latest_tx_block_out - e.earliest_tx_block_out))
  END AS out_degree_per_block_out,
  CASE
    WHEN (COALESCE (e.latest_tx_block_out - e.earliest_tx_block_out, 0)) = 0 THEN 0
    ELSE e.total_tx_value_out / ((e.latest_tx_block_out - e.earliest_tx_block_out))
  END AS tx_value_per_block_out,
  COALESCE(ei.num_transactions_in, 0) AS num_transactions_in,
  COALESCE(ei.earliest_tx_block_in,0) AS earliest_tx_block_in,
  COALESCE(ei.latest_tx_block_in,0)  AS latest_tx_block_in,
  COALESCE(ei.max_tx_value_in,0) AS max_tx_value_in,
  COALESCE(ei.min_tx_value_in,0) AS min_tx_value_in,
  COALESCE(ei.total_tx_value_in,0) AS total_tx_value_in,
  COALESCE(ei.avg_tx_value_in,0) AS avg_tx_value_in,
  COALESCE(ei.in_degree, 0) AS in_degree,
  (COALESCE(ei.latest_tx_block_in - ei.earliest_tx_block_in, 0)) AS time_span_in,
  CASE
    WHEN (COALESCE(ei.latest_tx_block_in - ei.earliest_tx_block_in, 0)) = 0 THEN 0
  ELSE ei.in_degree / ((COALESCE(ei.latest_tx_block_in - ei.earliest_tx_block_in, 0)))
  END AS indegree_per_block_in,
  CASE
    WHEN (COALESCE(ei.latest_tx_block_in - ei.earliest_tx_block_in, 0)) = 0 THEN 0
    ELSE ei.total_tx_value_in / ((COALESCE(ei.latest_tx_block_in - ei.earliest_tx_block_in, 0)))
  END AS tx_value_per_block_in
FROM
  layerzero_data l
  LEFT JOIN layerzero_to_eth leth on l.addr=leth.addr
  LEFT JOIN ethereum_tx_data e ON l.addr = e.addr 
  LEFT JOIN ethereum_in_tx_data ei on l.addr = ei.addr
  -- CHUNK DATA FOR FLIPSIDE DOWNLOAD
  QUALIFY RANK() OVER (
    ORDER BY
      l.addr ASC NULLS LAST
  ) BETWEEN 400000
  AND 500000
ORDER BY
  l.addr;
