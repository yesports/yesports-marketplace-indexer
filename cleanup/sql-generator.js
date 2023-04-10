const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/<DBNAME>?ssl=true';
const db = pgp(cn);
const fs = require("fs").promises;
const Web3 = require("web3");
const ABIS = require("../utils/abis.js");
const { CHAINS, CHAIN_LIST } = require("./utils/chains.js");
const CHAIN_NAME = "polygon";
const chainObject = CHAINS[CHAIN_NAME];

const providerMATIC = new Web3.providers.WebsocketProvider(chainObject?.rpc, {
        clientConfig: {
            maxReceivedFrameSize: 100000000,
            maxReceivedMessageSize: 100000000,
            keepalive: true,
            keepaliveInterval: -1
        },

        reconnect: {
            auto: true,
            delay: 50000000,
            maxAttempts: 5,
            onTimeout: true
        },

        timeout: 30000000
    })

const web3polygon = new Web3(providerMATIC);

async function main() {
    let marketAsks = [];
    const collections = JSON.parse(await fs.readFile('../utils/collections.json'));
    let collectionAddresses = {};
    let collectionChains = {};

    for (let [key, val] of Object.entries(collections)) {
        collectionAddresses[val['contractAddress']] = val['title'];
	    collectionChains[val['contractAddress']] = val['chain']; 
    }

    let oldAsks = [];
    marketAsks = await db.manyOrNone('SELECT "tokenId", "collectionId", "timestamp", "tokenNumber", "transactionHash" FROM "asks"');
    console.log("Listed tokens: ", marketAsks.length)
    if (marketAsks.length > 0) {
        console.log("Processing token list..");
        try {
            while (marketAsks.length > 0) {
                ask = marketAsks[0];
                if (!(ask['collectionId'] in collectionAddresses)) {
                    marketAsks.shift();
                    continue;
                }

                try {
                    const marketplaceContract = new web3polygon.eth.Contract(ABIS.MARKET, chainObject?.marketplace_contract_address);
                    const listingHash = await marketplaceContract.methods.currentListingOrderHash(asks['collectionId'], asks['tokenNumber']).call();
                    if (listingHash === '0x0000000000000000000000000000000000000000000000000000000000000000') {
                        // no listing found on contract
                        oldAsks.push(ask);
                        marketAsks.shift();
                        continue;
                    }
                    
                    //found listing, check for validity
                    const validListing = await marketplaceContract.methods.isValidListing(listingHash).call();
                    
                    if (!validListing) {
                        oldAsks.push(ask);
                    }
                } catch (e) {
                    console.log(e);
                    console.log('error thrown when checking ', asks['collectionId'], asks['tokenNumber']);
                }

                marketAsks.shift();
            }
        } catch (e) {
            console.log(e);
            console.log("Retrying...");
            await sleep(120000);
        }
    }

    console.log("Token Transfers after their listing: ", oldAsks.length);

    let oldContracts = {};
    let tokenIds = [];
    for (let oldAsk of oldAsks) {
        if (!(oldAsk['collectionId'] in oldContracts)) {
            oldContracts[oldAsk['collectionId']] = {
                title: collectionAddresses[oldAsk['collectionId']],
                count: 0,
                entities: []
            }
        }

        oldContracts[oldAsk['collectionId']]['count']++;
        oldContracts[oldAsk['collectionId']]['entities'].push(oldAsk['tokenNumber']);
        tokenIds.push(oldAsk['tokenId']);
    }

    console.log(oldContracts);

    if (tokenIds.length) {
        let date_ob = new Date();
        let today = date_ob.getFullYear() + '-' + ("0" + (date_ob.getMonth() + 1)).slice(-2) + '-' + ("0" + date_ob.getDate()).slice(-2) + "-" + date_ob.getHours() + "-" + date_ob.getMinutes() + "-" + date_ob.getSeconds();
        await fs.writeFile(`./deletions/${today}.sql`, `DELETE FROM "asks" WHERE "tokenId" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});
        await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "tokens" SET "currentAsk" = 0 WHERE "id" IN ('${tokenIds.join("','")}');`, { flag: 'a+' }, err => {});

        for (let collectionId in oldContracts) {
            await fs.writeFile(`./deletions/${today}.sql`, `UPDATE "collections" SET "ceilingPrice" = (SELECT MAX("value") FROM "asks" WHERE "collectionId" = '${collectionId}'), "floorPrice" = (SELECT MIN("value") FROM "asks" WHERE "collectionId" = '${collectionId}') WHERE "id" = '${collectionId}';`, { flag: 'a+' }, err => {});
        }
    }   
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

main().then((e => {
    console.log('Done'); 
    process.exit()
}))
