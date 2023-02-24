const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const Web3 = require("web3");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/<DBNAME>';
const db = pgp(cn);
const ABIS = require("./utils/abis.js");
const CHAIN_NAME = "polygon";
const CHAINS = require("./utils/chains.js");
const chainObject = CHAINS[CHAIN_NAME];

const blockBatch = 500;

let methodSignatures = [];
const trackActivity = true;

/*****************
       SETUP
******************/

// Get our web3 provider setup
const web3 = new Web3(new Web3.providers.WebsocketProvider(chainObject.rpc, {
    clientConfig: {
        maxReceivedFrameSize: 100000000,
        maxReceivedMessageSize: 100000000,
        keepalive: true,
        keepaliveInterval: 60000
    },

    reconnect: {
        auto: true,
        delay: 50000,
        maxAttempts: 5,
        onTimeout: true
    },

    timeout: 30000
}));

// Get hashes of all signatures / function names so we can easily tell what function was called from a tx
ABIS.MARKET.map(function (abi) {
    if (abi.name) {
        const signature = web3.utils.sha3(
            abi.name +
            "(" +
            abi.inputs
                .map(_typeToString)
                .join(",") +
            ")"
        );
        if (abi.type !== "event") {
            methodSignatures[signature.slice(2, 10)] = abi.name;
        }
    }
});

ABIS.FUNGIBLE_MARKET.map(function (abi) {
    if (abi.name) {
        const signature = web3.utils.sha3(
            abi.name +
            "(" +
            abi.inputs
                .map(_typeToString)
                .join(",") +
            ")"
        );
        if (abi.type !== "event") {
            methodSignatures[signature.slice(2, 10)] = abi.name;
        }
    }
});

// Create requisite contract objects
const marketPlaceContract = new web3.eth.Contract(ABIS.MARKET, chainObject.marketplace_contract_address);
const fungibleMarketPlaceContract = new web3.eth.Contract(ABIS.FUNGIBLE_MARKET, chainObject.fungible_marketplace_contract_address);

/*****************
     ENTRYPOINT
******************/
startListening();

async function startListening() {
    startListeningMarketplace();
    startListeningFungibleMarketplace();
    // startListeningHolders();
}



/*****************
    MARKETPLACE
******************/
async function startListeningMarketplace() {
    let startBlock = parseInt(await fs.readFile("last_block_polygon.txt"));

    if (startBlock == 0) {
        startBlock = 38760423;
    }

    let lastBlock = await web3.eth.getBlockNumber();

    let endBlock = startBlock + blockBatch;
    if (endBlock > lastBlock) {
        endBlock = lastBlock;
    }

    handleMarketplaceLogs(startBlock, endBlock, lastBlock);
}


async function handleMarketplaceLogs(startBlock, endBlock, lastBlock) {
    try {
        while (true) {
            console.log(startBlock, endBlock, lastBlock);

            let events = await marketPlaceContract.getPastEvents("allEvents", { 'fromBlock': startBlock, 'toBlock': endBlock });

            let sortedEvents = events.reverse().sort(function (x, y) {
                return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex;
            });

            for (let row of sortedEvents) {
                if (row.removed) {
                    continue;
                }

                row['transactionEventHash'] = row['transactionHash'] + "-" + row['transactionIndex'] + "-" + row['logIndex'];

                const txrow = await db.oneOrNone('SELECT * FROM "transactions" WHERE "id" = $1', [row['transactionEventHash']]);

                console.log(row['transactionEventHash']);
                if (txrow !== null) {
                    console.log("skipping");
                    continue;
                }

                let transactionHandled = true;

                if (row.event == "TokenListed") {
                    await handleTokenListed(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TokenDelisted") {
                    await handleTokenDelisted(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TokenPurchased") {
                    await handleTokenPurchased(row);
                } else if (row.event == "BidPlaced" || row.event == "OfferPlaced") {
                    await handledBidPlaced(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "BidCancelled" || row.event == "OfferCancelled") {
                    await handleBidCancelled(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else {
                    transactionHandled = false;
                }

                if (transactionHandled) {
                    await handleTransaction(row);
                }
            }

            startBlock = endBlock;
            await fs.writeFile("./last_block_polygon.txt", "" + startBlock);
            if (startBlock >= lastBlock) {
                endBlock = await web3.eth.getBlockNumber();
                await sleep(10000);
            } else {
                endBlock += blockBatch;
                if (endBlock > lastBlock) {
                    endBlock = lastBlock;
                }
            }
        }
    } catch (e) {
        console.log(e);
        handleMarketplaceLogs(startBlock, endBlock, lastBlock);
    }
}


async function handleTokenListed(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    const CA = row['returnValues']['token'];
    let block = await web3.eth.getBlock(row['blockNumber']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let event_id = `${tx['from']}-TOKENLISTING-${block['timestamp']}-${row['transactionHash']}`;

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("event_id", "user_address", "activity", "chain_name", "token_address", "token_id", "amount", "time_stamp") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', 
            [event_id, tx['from'], "TOKEN_LISTING", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], price.toString(), block['timestamp']]
        ); 
    } catch (e) { console.log(e); }

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', CA);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName") VALUES ($1, $2, $3, $4, $5)', [CA, 0, 0, 0, CHAIN_NAME]);
        collection = {
            'id': CA,
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0,
            'chainName': CHAIN_NAME
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "heighestBid") VALUES ($1, $2, $3, $4, $5, $6)', 
            [id, row['returnValues']['id'], row['returnValues']['token'], price.toString(), 0, 0]
        );
        token = {
            'id': id,
            'tokenNumber': row['returnValues']['id'],
            'collectionId': row['returnValues']['token'],
            'currentAsk': price,
            'lowestBid': 0,
            'heighestBid': 0
        };
    } else {
        await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [price.toString(), id]);
        token['currentAsk'] = price;
    }

    if (web3.utils.toBN(collection['ceilingPrice']).lte(price)) {
        await db.any('UPDATE "collections" SET "ceilingPrice" = $1 WHERE "id" = $2', [price.toString(), row['returnValues']['token']]);
        collection['ceilingPrice'] = price;
    }

    if (web3.utils.toBN(collection['floorPrice']).gte(price)) {
        await db.any('UPDATE "collections" SET "floorPrice" = $1 WHERE "id" = $2', [price.toString(), row['returnValues']['token']]);
        collection['floorPrice'] = price;
    }

    // SAVE CURRENT ASK
    await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);
    await db.any('INSERT INTO "asks" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
        [id, row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), row['returnValues']['timestamp'], row['transactionHash'], tx['from'], CHAIN_NAME, row['returnValues']['listingHash'], web3.utils.toBN(row['returnValues']['expiry']).toString()]
    );

    // SAVE CURRENT ASK INTO HISTORY
    await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
        [row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), row['returnValues']['timestamp'], 0, row['transactionHash'], tx['from'], CHAIN_NAME, row['returnValues']['listingHash'], web3.utils.toBN(row['returnValues']['expiry']).toString()]
    );

    console.log(`[TOKEN LISTED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}}; price: ${price}`);
}


async function handleTokenDelisted(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let block = await web3.eth.getBlock(row['blockNumber']);
    let event_id = `${tx['from']}-TOKENLISTING-${block['timestamp']}-${row['transactionHash']}`;

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("event_id", "user_address", "activity", "chain_name", "token_address", "token_id", "amount", "time_stamp") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', 
            [event_id, tx['from'], "TOKEN_DELISTING", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], 0, block['timestamp']]
        ); 
    } catch (e) { console.log(e); }

    // UDPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        console.log("Token not in database");
    }

    await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [0, id]);

    // REMOVE CURRENT ASK
    await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);

    // SAVE DELIST TO ASK HISTORY
    await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
        [row['returnValues']['token'], row['returnValues']['id'], id, 0, row['returnValues']['timestamp'], 0, row['transactionHash'], tx['from'], CHAIN_NAME, row['returnValues']['listingHash'], 0]
    );

    // UPDATE COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        console.log("Collection not in database");
    }

    let [floorPrice, ceilingPrice] = await getCollectionPrices(row['returnValues']['token']);
    await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2 WHERE "id" = $3', [floorPrice, ceilingPrice, row['returnValues']['token']]);

    console.log(`[TOKEN DELISTED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']};`);
}


async function handleTokenPurchased(row) {
    let block = await web3.eth.getBlock(row['blockNumber']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    row['timestamp'] = block['timestamp'];
    let event_id = `${tx['from']}-TOKENPURCHASED-${block['timestamp']}-${row['transactionHash']}`;

    if (methodSignatures[tx.input.slice(2, 10)] == "acceptOffer") {
        const id = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}`;
        const fillId = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}-${row['transactionHash']}`;

        // For user activity panel
        if (trackActivity) try { 
            await db.any('INSERT INTO "activityHistories" ("event_id", "user_address", "activity", "chain_name", "token_address", "token_id", "amount", "time_stamp") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', 
                [event_id, tx['from'], "OFFER_ACCEPTED", CHAIN_NAME, row['returnValues']['collection'], row['returnValues']['tokenId'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp']]
            ); 
        } catch (e) { console.log(e); }

        // REMOVE CURRENT BID
        let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 AND "offerHash" = $4 ORDER BY "timestamp" DESC LIMIT 1', [id, row['returnValues']['newOwner'], web3.utils.toBN(row['returnValues']['price']).toString(), row['returnValues']['tradeHash']]);
        if (bid !== null) {
            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type", "chainName", "tradeHash", "seller") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)', [fillId, row['returnValues']['collection'], row['returnValues']['tokenId'], id, web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['newOwner'], 'bid', CHAIN_NAME, row['returnValues']['tradeHash'], row['returnValues']['oldOwner']]);

            // REMOVE BID
            await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

            // UPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            let [lowestBid, heighestBid] = await getTokenPrices(id);
            await db.any('UPDATE "tokens" SET "lowestBid" = $1, "heighestBid" = $2 WHERE "id" = $3', [lowestBid, heighestBid, id]);

            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['collection']]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);

            console.log(`[FILL BID] tx: ${row['transactionHash']}; token: ${row['returnValues']['tokenId']}; collection: ${row['returnValues']['collection']}; from: ${row['returnValues']['newOwner']}; price: ${row['returnValues']['price']}`);
        } else {
            console.log("Bid not in database");
        }
    } else {
        const id = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}`;
        const fillId = `${row['returnValues']['collection']}-${row['returnValues']['tokenId']}-${row['transactionHash']}`;

        // For user activity panel
        if (trackActivity) try { 
            await db.any('INSERT INTO "activityHistories" ("event_id", "user_address", "activity", "chain_name", "token_address", "token_id", "amount", "time_stamp") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', 
                [event_id, tx['from'], "TOKEN_PURCHASED", CHAIN_NAME, row['returnValues']['collection'], row['returnValues']['tokenId'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp']]
            ); 
        } catch (e) { console.log(e); }

        let filledAsk = await db.oneOrNone('SELECT * FROM "askHistories" WHERE "tokenId" = $1 AND "value" = $2 AND "accepted" = $3 AND "listingHash" = $4 ORDER BY "timestamp" DESC LIMIT 1', [id, web3.utils.toBN(row['returnValues']['price']).toString(), 0, row['returnValues']['tradeHash']]);
        if (filledAsk !== null) {
            const askHistoryId = filledAsk['id'];

            // UDPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [0, id]);

            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type", "chainName", "tradeHash", "seller") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)', [fillId, row['returnValues']['collection'], row['returnValues']['tokenId'], id, web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['newOwner'], 'ask', CHAIN_NAME, row['returnValues']['tradeHash'], row['returnValues']['oldOwner']]);

            // UPDATE OLD ASK HISTORY
            await db.any('UPDATE "askHistories" SET "accepted" = $1 WHERE "id" = $2', [1, askHistoryId]);

            // REMOVE CURRENT ASK
            await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);

            // SAVE DELIST TO ASK HISTORY
            await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
            [row['returnValues']['collection'], row['returnValues']['tokenId'], id, 0, block['timestamp'], 0, row['transactionHash'], row['returnValues']['oldOwner'], CHAIN_NAME, row['returnValues']['tradeHash'], row['returnValues']['expiry']]);
            
            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['collection']]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            let [floorPrice, ceilingPrice] = await getCollectionPrices(row['returnValues']['collection']);
            await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2, "volumeOverall" = $3 WHERE "id" = $4', [floorPrice, ceilingPrice, ((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);
            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [((web3.utils.toBN(collection['volumeOverall'])).add(web3.utils.toBN(row['returnValues']['price']))).toString(), row['returnValues']['collection']]);

            console.log(`[FILL ASK] tx: ${row['transactionHash']}; token: ${row['returnValues']['tokenId']}; collection: ${row['returnValues']['collection']}; price: ${row['returnValues']['price']}`)
        } else {
            console.log("Ask not in database");
        }
    }
}


async function handledBidPlaced(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    const bidId = `${row['returnValues']['token']}-${row['returnValues']['id']}-${row['returnValues']['buyer']}-${row['transactionHash']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let block = await web3.eth.getBlock(row['blockNumber']);
    let event_id = `${tx['from']}-OFFERPLACED-${block['timestamp']}-${row['transactionHash']}`;

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName") VALUES ($1, $2, $3, $4, $5)', [row['returnValues']['token'], 0, 0, 0, CHAIN_NAME]);
        collection = {
            'id': row['returnValues']['token'],
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0,
            'chainName': CHAIN_NAME
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "heighestBid") VALUES ($1, $2, $3, $4, $5, $6)', [id, row['returnValues']['id'], row['returnValues']['token'], price.toString(), 0, 0]);
        token = {
            'id': id,
            'tokenNumber': row['returnValues']['id'],
            'collectionId': row['returnValues']['token'],
            'currentAsk': 0,
            'lowestBid': price,
            'heighestBid': price
        };
    } else {
        if (web3.utils.toBN(token['lowestBid']).lte(price)) {
            await db.any('UPDATE "tokens" SET "lowestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['lowestBid'] = price;
        }

        if (web3.utils.toBN(token['heighestBid']).gte(price)) {
            await db.any('UPDATE "tokens" SET "heighestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['heighestBid'] = price;
        }
    }

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("event_id", "user_address", "activity", "chain_name", "token_address", "token_id", "amount", "time_stamp") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', 
            [event_id, tx['from'], "OFFER_PLACED", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['tokenId'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp']]
        ); 
    } catch (e) { console.log(e); }

    // SAVE BID
    await db.any('INSERT INTO "bids" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "transactionHash", "expiry", "offerHash", "chainName", "seller") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)',
        [bidId, row['returnValues']['token'], row['returnValues']['id'], id, price.toString(), block['timestamp'], row['returnValues']['buyer'], row['transactionHash'], web3.utils.toBN(row['returnValues']['expiry']).toString(), row['returnValues']['offerHash'], CHAIN_NAME, row['returnValues']['potentialSeller'] ]);

    console.log(`[BID PLACED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}; price: ${price}}`)

}


async function handleBidCancelled(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;

    // REMOVE CURRENT BID
    let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 AND "offerHash" = $4 ORDER BY "timestamp" ASC LIMIT 1', [id, row['returnValues']['buyer'], web3.utils.toBN(row['returnValues']['price']).toString(), row['returnValues']['offerHash']]);
    if (bid !== null) {
        await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

        // UPDATE TOKEN
        let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
        if (token === null) {
            console.log("Token not in database");
        }

        let [lowestBid, heighestBid] = await getTokenPrices(id);
        await db.any('UPDATE "tokens" SET "lowestBid" = $1, "heighestBid" = $2 WHERE "id" = $3', [lowestBid, heighestBid, id]);

        console.log(`[BID CANCELLED] tx: ${row['transactionHash']}; token: ${row['returnValues']['id']}; collection: ${row['returnValues']['token']}; from: ${row['returnValues']['buyer']}; price: ${row['returnValues']['price']}`);
    } else {
        console.log("Bid not in database");
    }
}

async function handleTransaction(row) {

    try {
        await db.any('INSERT INTO "transactions" ("id", "blockNumber", "timestamp", "chainName") VALUES ($1, $2, $3, $4)',
            [row['transactionEventHash'], row['blockNumber'], row['timestamp'], CHAIN_NAME]);

    } catch (e) {
        console.log("Error updating transactions table. Trying without chain name.");
        console.log(e);
        await db.any('INSERT INTO "transactions" ("id", "blockNumber", "timestamp") VALUES ($1, $2, $3)',
        [row['transactionEventHash'], row['blockNumber'], row['timestamp']]);
    }

}


/*****************
      HOLDERS
******************/

// async function startListeningHolders() {
//     lastBlock = await web3.eth.getBlockNumber();
//     let collections = JSON.parse(await fs.readFile('./utils/collections.json'));

//     for (let key in collections) {
//         let startBlockQuery = await db.oneOrNone('SELECT "value" FROM "meta" WHERE "name" = $1', ['last_block_' + key]);
//         let startBlock = collections[key]?.startBlock ?? 0;
//         if (startBlockQuery === null) {
//             await db.any('INSERT INTO "meta" ("name", "value", "timestamp") VALUES ($1, $2, $3)', ['last_block_' + key, startBlock, Math.floor(Date.now() / 1000)]);
//         } else {
//             startBlock = parseInt(startBlockQuery['value']);
//         }

//         let endBlock = startBlock + blockBatch;
//         if (endBlock > lastBlock) {
//             endBlock = lastBlock;
//         }

//         handleCollectionTransfers(key, startBlock, endBlock, collections);
//     }
// }

// async function handleCollectionTransfers(key, startBlock, endBlock, collections) {
//     let collection = collections[key];
//     if (collection['chain'] !== 'polygon') return;
//     try {
//         while (true) {
//             console.log('Getting Transfer events for ' + collection['title'] + ' (' + collection['contractAddress'] + ') ' + startBlock + '/' + endBlock);

//             if (collection['isERC1155']) {
//                 await handleTransfer1155(key, startBlock, endBlock, collection);
//             } else { //ERC721
//                 await handleTransfer721(key, startBlock, endBlock, collection);
//             }
//         }
//     } catch (e) {
//         console.log(e);
//         handleCollectionTransfers(key, startBlock, endBlock, collections);
//     }
// }








/*****************
     HELPERS
******************/

function _typeToString(input) {
    if (input.type === "tuple") {
        return "(" + input.components.map(_typeToString).join(",") + ")";
    }
    return input.type;
}


function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function getTokenPrices(id) {
    let floorPrice = null;
    let ceilingPrice = null;

    let bids = await db.manyOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1', [id]);
    if (bids.length > 0) {
        for (let bid of bids) {
            if (floorPrice === null || (web3.utils.toBN(bid['value'])).lte(web3.utils.toBN(floorPrice))) {
                floorPrice = web3.utils.toBN(bid['value']);
            }
            if (ceilingPrice === null || (web3.utils.toBN(bid['value'])).gte(web3.utils.toBN(ceilingPrice))) {
                ceilingPrice = web3.utils.toBN(bid['value']);
            }
        }
    }

    if (floorPrice === null) {
        floorPrice = web3.utils.toBN(0);
    }
    if (ceilingPrice === null) {
        ceilingPrice = web3.utils.toBN(0);
    }

    return [floorPrice.toString(), ceilingPrice.toString()];
}

async function getCollectionPrices(collectionId) {
    let floorPrice = null;
    let ceilingPrice = null;

    let asks = await db.manyOrNone('SELECT * FROM "asks" WHERE "collectionId" = $1', [collectionId]);
    if (asks.length > 0) {
        for (let ask of asks) {
            if (floorPrice === null || (web3.utils.toBN(ask['value'])).lte(web3.utils.toBN(floorPrice))) {
                floorPrice = web3.utils.toBN(ask['value']);
            }
            if (ceilingPrice === null || (web3.utils.toBN(ask['value'])).gte(web3.utils.toBN(ceilingPrice))) {
                ceilingPrice = web3.utils.toBN(ask['value']);
            }
        }
    }

    if (floorPrice === null) {
        floorPrice = web3.utils.toBN(0);
    }
    if (ceilingPrice === null) {
        ceilingPrice = web3.utils.toBN(0);
    }

    return [floorPrice.toString(), ceilingPrice.toString()];
}


// async function handleTransfer721(key, startBlock, endBlock, collection) {
//     let contract = new web3.eth.Contract(ABIS.NFT, collection['contractAddress']);

//     let events = await contract.getPastEvents("Transfer", { 'fromBlock': startBlock, 'toBlock': endBlock });

//     let sortedEvents = events.reverse().sort(function (x, y) {
//         return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex;
//     });

//     for (let row of sortedEvents) {
//         if (row.removed) {
//             continue;
//         }

//         if (row['blockNumber'] in blockTimestamps) {
//             row['timestamp'] = blockTimestamps[row['blockNumber']]
//         } else {
//             let block = await web3.eth.getBlock(row['blockNumber']);
//             row['timestamp'] = block['timestamp'];
//             blockTimestamps[row['blockNumber']] = block['timestamp'];
//         }

//         let id = collection['contractAddress'] + '-' + row['returnValues']['2'];

//         let token = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [id]);
//         if (token === null) {
//             await db.any('INSERT INTO "holders" ("id", "tokenNumber", "collectionId", "currentOwner", "lastTransfer", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', [id, row['returnValues']['2'], collection['contractAddress'], row['returnValues']['1'], row['timestamp'], CHAIN_NAME]);
//         } else {
//             await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2 WHERE "id" = $3', [row['returnValues']['1'], row['timestamp'], id]);
//         }
//     }

//     startBlock = endBlock;
//     await db.any('UPDATE "meta" SET "value" = $1, "timestamp" = $2 WHERE "name" = $3', [startBlock, Math.floor(Date.now() / 1000), 'last_block_' + key]);

//     if (startBlock >= lastBlock) {
//         endBlock = await web3.eth.getBlockNumber();
//         await sleep(120000);
//     } else {
//         endBlock += blockBatch;
//         if (endBlock > lastBlock) {
//             endBlock = lastBlock;
//         }
//         await sleep(800);
//     }
// }

//TODO
// async function handleTransfer1155(key, startBlock, endBlock, collection) {
//     let contract = new web3.eth.Contract(ABIS.NFT1155, collection['contractAddress']);

//     let singleTransfers = await contract.getPastEvents("TransferSingle", { 'fromBlock': startBlock, 'toBlock': endBlock });
//     let batchTransfers = await contract.getPastEvents("TransferBatch", { 'fromBlock': startBlock, 'toBlock': endBlock });

//     let transferTypes = {};
//     singleTransfers.forEach((transferEvent) => {
//         transferTypes[`${transferEvent.transactionHash}-${transferEvent.logIndex}`] = 'single';
//     })
//     batchTransfers.forEach((transferEvent) => {
//         transferTypes[`${transferEvent.transactionHash}-${transferEvent.logIndex}`] = 'batch';
//     })

//     let events = singleTransfers.concat(batchTransfers);

//     let sortedEvents = events.reverse().sort(function (x, y) {
//         return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex;
//     });

//     for (let row of sortedEvents) {
//         if (row.removed) {
//             continue;
//         }

//         if (row['blockNumber'] in blockTimestamps) {
//             row['timestamp'] = blockTimestamps[row['blockNumber']]
//         } else {
//             let block = await web3.eth.getBlock(row['blockNumber']);
//             row['timestamp'] = block['timestamp'];
//             blockTimestamps[row['blockNumber']] = block['timestamp'];
//         }

//         let id = collection['contractAddress'] + '-' + row['returnValues']['2'];

//         let token = await db.oneOrNone('SELECT * FROM "holders" WHERE "id" = $1', [id]);
//         if (token === null) {
//             await db.any('INSERT INTO "holders" ("id", "tokenNumber", "collectionId", "currentOwner", "lastTransfer", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', [id, row['returnValues']['2'], collection['contractAddress'], row['returnValues']['1'], row['timestamp'], CHAIN_NAME]);
//         } else {
//             await db.any('UPDATE "holders" SET "currentOwner" = $1, "lastTransfer" = $2 WHERE "id" = $3', [row['returnValues']['1'], row['timestamp'], id]);
//         }
//     }

//     startBlock = endBlock;
//     await db.any('UPDATE "meta" SET "value" = $1, "timestamp" = $2 WHERE "name" = $3', [startBlock, Math.floor(Date.now() / 1000), 'last_block_' + key]);

//     if (startBlock >= lastBlock) {
//         endBlock = await web3.eth.getBlockNumber();
//         await sleep(120000);
//     } else {
//         endBlock += blockBatch;
//         if (endBlock > lastBlock) {
//             endBlock = lastBlock;
//         }
//         await sleep(800);
//     }
// }