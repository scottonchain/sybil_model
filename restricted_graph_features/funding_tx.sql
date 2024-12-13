WITH layerzero_eth_addresses AS (
    -- Select sender addresses from LayerZero snapshot where source_chain is Ethereum
    SELECT DISTINCT sender_wallet
    FROM external.layerzero.fact_transactions_snapshot
    WHERE source_chain = 'Ethereum'
),
first_gas_provision_blocks AS (
    -- Select the first gas provision to each layerzero_eth_address
    SELECT
        ft.to_address AS activated_address,
        MIN(ft.block_number) AS block_number
    FROM ethereum.core.ez_native_transfers ft
    WHERE ft.to_address IN (SELECT sender_wallet FROM layerzero_eth_addresses)
      AND ft.amount > 0 -- Ensuring it's a non-zero transfer (gas provision)
      AND ft.block_timestamp < TO_TIMESTAMP_NTZ('2024-05-01 00:00:00')
    GROUP BY ft.to_address
),
first_gas_provisions AS (
    -- assume no duplicate from/to in blocks
    SELECT
        ft.tx_hash as tx_hash,
        ft.block_number as block_number,
        ft.origin_from_address AS gas_provider,
        ft.to_address AS activated_address,
        ft.amount AS gas_provision_amount,
        --ft.tx_fee AS transaction_fee,
        ft.block_timestamp AS first_gas_provision_time
      FROM ethereum.core.ez_native_transfers ft  JOIN first_gas_provision_blocks fgpb 
      on ft.block_number = fgpb.block_number AND ft.to_address = fgpb.activated_address
    AND ft.amount > 0 -- Ensuring it's a non-zero transfer (gas provision)
      AND ft.from_address != ft.to_address
      AND ft.block_timestamp < TO_TIMESTAMP_NTZ('2024-05-01 00:00:00')
)
SELECT *
FROM first_gas_provisions fg
QUALIFY RANK() OVER (ORDER BY activated_address) BETWEEN 0 and 100000
ORDER BY activated_address

