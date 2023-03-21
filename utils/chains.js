const CHAINS = {
    "polygon": {
        "chain_name": "Polygon",
        "chain_id": 137,
        "rpc": "wss://polygon-mainnet.g.alchemy.com/v2/M8n-tFUkbCemboXyAZbHbTURRTLIKBia",
        "marketplace_contract_address": "0x80D385e56cBF3C1cA27A511A7Eb63a77Dc681484",
        "fungible_marketplace_contract_address": "0xDFFc89E4702Da0129a7B16023d13B4A5AB8FB522",
        "testnet": false
    },
    "mumbai": {
        "chain_name": "Polygon Testnet",
        "chain_id": 80001,
        "rpc": "wss://rpc-mumbai.matic.today", //todo
        "marketplace_contract_address": "0x0517b13E5a79Fda0D790d3CC6473815476d96814",
        "fungible_marketplace_contract_address": "0x3a05323bF50a23AAF0CbFA2fbf2E328999e8489a",
        "testnet": true
    },
};

module.exports = { CHAINS };