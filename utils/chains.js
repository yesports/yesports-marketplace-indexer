const CHAINS = {
    "polygon": {
        "chain_name": "Polygon",
        "chain_id": 137,
        "rpc": "wss://polygon-mainnet.g.alchemy.com/v2/M8n-tFUkbCemboXyAZbHbTURRTLIKBia",
        // "marketplace_contract_address": "0x80D385e56cBF3C1cA27A511A7Eb63a77Dc681484",
        "marketplace_contract_address": "0x673f64a2b7CCf467d4edca665080354b064D273f",
        // "fungible_marketplace_contract_address": "0xDFFc89E4702Da0129a7B16023d13B4A5AB8FB522",
        "fungible_marketplace_contract_address": "0xbea770b77477a9C46b1F663bCa912B13Bc9f950a",
        "startBlock": 39645101,
        "sleeptime": 3000,
        "testnet": false
    },
    "ethereum": {
        "chain_name": "Ethereum",
        "chain_id": 1,
        "rpc": "wss://eth-mainnet.g.alchemy.com/v2/9YdL81vAxv7Jt9Dk_4QMTw9sWw2RM-Q0",
        "marketplace_contract_address": "0x89F6467492658E36e9F7812ebFC030C1F96C3D73",
        "fungible_marketplace_contract_address": "0x72d60c0B0204b8Ee2fA300c225897A5ad001259e",
        "startBlock": 17013251,
        "sleeptime": 10000,
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

const CHAIN_LIST = ['polygon', 'ethereum'];

module.exports = { CHAINS, CHAIN_LIST };