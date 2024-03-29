require('dotenv').config()
const Web3 = require("web3");
const fs = require("fs").promises;
const timers = require('timers/promises');
const pgp = require("pg-promise")({});
const cn = `postgres://${process.env.DBUSER}:${process.env.DBPASS}@${process.env.DBHOST}:5432/${process.env.DBNAME}${process.env.USE_SSL === "true" ? "?ssl=true" : ""}`;
const db = pgp(cn);
const ABIS = require("./utils/abis.js");
const { program, Option } = require('commander');
const { CHAINS, CHAIN_LIST } = require("./utils/chains.js");

/*****************
    CHAIN SETUP
******************/

program
  .version('1.0.0', '-v, --version')
  .addOption(new Option('-c, --chain <value>', 'chain name ~ should be present in chains.js').choices(CHAIN_LIST))
  .parse();

const options = program.opts();
const CHAIN_NAME = options.chain;
let chainObject = CHAINS[CHAIN_NAME];
const blockBatch = chainObject?.blockBatch ?? 500;

console.log("Starting Yesports Indexer for " + CHAIN_NAME);

let methodSignatures = [];
const trackActivity = true;

/*****************
    WEB3 SETUP
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
// NLL
const nllContract = chainObject?.chain_name === "Ethereum" ? new web3.eth.Contract(ABIS.NLL, chainObject.nll_contract_address) : null;

/*****************
     ENTRYPOINT
******************/
startListening();

async function startListening() {
    chainObject = await setupChain(chainObject);
    startListeningMarketplace();
}



/*****************
    MARKETPLACE
******************/
async function startListeningMarketplace() {
    // let startBlock = parseInt(await fs.readFile(`last_block_${CHAIN_NAME}.txt`));
    let startBlock = chainObject?.startBlock ?? 0;

    // if (startBlock == 0) {
    //     startBlock = chainObject?.startBlock ?? 0;
    // }
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
            console.log('Start:', startBlock, 'End:', endBlock, 'Last:', lastBlock);

            // let oldEvents = await oldMarketplaceContract.getPastEvents("allEvents", { 'fromBlock': startBlock, 'toBlock': endBlock });
            let events = await marketPlaceContract.getPastEvents("allEvents", { 'fromBlock': startBlock, 'toBlock': endBlock });
            let fungiEvents = await fungibleMarketPlaceContract.getPastEvents("allEvents", { 'fromBlock': startBlock, 'toBlock': endBlock});
            let nllEvents = chainObject?.chain_name === "Ethereum" 
                ? await nllContract.getPastEvents("allEvents", {'fromBlock': startBlock, 'toBlock': endBlock}) 
                : [];

            let sortedEvents = events.reverse().concat(fungiEvents.reverse()).concat(nllEvents.reverse()).sort(function (x, y) {
                return x.blockNumber - y.blockNumber || x.transactionIndex - y.transactionIndex || x.logIndex - y.logIndex || x.transactionHash - y.transactionHash;
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
                } else if (row.event == "CollectionModified") {
                    await handleCollectionModified(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TradeOpened") {
                    await handleTradeOpened(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TradeAccepted") {
                    await handleTradeAccepted(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "TradeCancelled") {
                    await handleTradeCancelled(row);
                    row['timestamp'] = row['returnValues']['timestamp'];
                } else if (row.event == "WinnerSet") {
                    await handleWinnerSet(row);
                    row['timestamp'] = row['returnValues']['setTime'];
                } else {
                    transactionHandled = false;
                }

                if (transactionHandled) {
                    await handleTransaction(row);
                }
            }

            startBlock = endBlock;
            // await fs.writeFile(`./last_block_${CHAIN_NAME}.txt`, "" + startBlock);
            await db.any('UPDATE "chains" SET "lastIndexedBlock" = $1 WHERE "name" = $2', [startBlock, CHAIN_NAME]);
            if (startBlock >= lastBlock) {
                endBlock = await web3.eth.getBlockNumber();
                await sleep(chainObject?.sleepTime ?? 10000);
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
    const tokenNumber = row['returnValues']['id'];
    let block = await web3.eth.getBlock(row['blockNumber']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let event_id = `${tx['from']}-TOKENLISTING-${block['timestamp']}-${row['transactionHash']}`;
    const expiry = web3.utils.toBN(row['returnValues']['expiry'] ?? 0);
    const listingHash = row['returnValues']['listingHash'] ?? "OLD_CONTRACT";

    // For user activity panel
    if (trackActivity) try {
        await handleListingTracking(event_id, tx, row, price, block);
    } catch (e) { console.log(e); }

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', CA);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName", "tradingEnabled", "isERC1155") VALUES ($1, $2, $3, $4, $5, $6, $7)', [CA, 0, 0, 0, CHAIN_NAME, true, false]);
        collection = {
            'id': CA,
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0,
            'chainName': CHAIN_NAME,
            'tradingEnabled': true
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
            [id, tokenNumber, CA, price.toString(), 0, 0, CHAIN_NAME]
        );
        token = {
            'id': id,
            'tokenNumber': tokenNumber,
            'collectionId': CA,
            'currentAsk': price,
            'lowestBid': 0,
            'highestBid': 0
        };
    } else {
        await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [price.toString(), id]);
        token['currentAsk'] = price;
    }

    if (web3.utils.toBN(collection['ceilingPrice']).lte(price)) {
        await db.any('UPDATE "collections" SET "ceilingPrice" = $1 WHERE "id" = $2', [price.toString(), CA]);
        collection['ceilingPrice'] = price;
    }

    if (web3.utils.toBN(collection['floorPrice']).gte(price)) {
        await db.any('UPDATE "collections" SET "floorPrice" = $1 WHERE "id" = $2', [price.toString(), CA]);
        collection['floorPrice'] = price;
    }

    // SAVE CURRENT ASK
    await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);
    await db.any('INSERT INTO "asks" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
        [id, CA, tokenNumber, id, price.toString(), row['returnValues']['timestamp'], row['transactionHash'], tx['from'], CHAIN_NAME, listingHash, expiry.toString()]
    );

    // SAVE CURRENT ASK INTO HISTORY
    await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
        [CA, tokenNumber, id, price.toString(), row['returnValues']['timestamp'], 0, row['transactionHash'], tx['from'], CHAIN_NAME, listingHash, expiry.toString()]
    );

    console.log(`[TOKEN LISTED] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}}; price: ${price}`);
}


async function handleTokenDelisted(row) {
    const id = `${row['returnValues']['token']}-${row['returnValues']['id']}`;
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['id'];
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let block = await web3.eth.getBlock(row['blockNumber']);
    let event_id = `${tx['from']}-TOKENLISTING-${block['timestamp']}-${row['transactionHash']}`;
    const listingHash = row['returnValues']['listingHash'] ?? "OLD_CONTRACT";

    // For user activity panel
    if (trackActivity) try { 
        await handleDelistingTracking(event_id, tx, row, block);
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
        [CA, tokenNumber, id, 0, row['returnValues']['timestamp'], 0, row['transactionHash'], tx['from'], CHAIN_NAME, listingHash, 0]
    );

    // UPDATE COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [CA]);
    if (collection === null) {
        console.log("Collection not in database");
    }

    let [floorPrice, ceilingPrice] = await getCollectionPrices(CA);
    await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2 WHERE "id" = $3', [floorPrice, ceilingPrice, CA]);

    console.log(`[TOKEN DELISTED] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA};`);
}


async function handleTokenPurchased(row) {
    const CA = row['returnValues']['collection'];
    const tokenNumber = row['returnValues']['tokenId'];
    const id = `${CA}-${tokenNumber}`;
    const fillId = `${CA}-${tokenNumber}-${row['transactionHash']}`;
    const tradeHash = row['returnValues']['tradeHash'] ?? "OLD_CONTRACT";
    const price = web3.utils.toBN(row['returnValues']['price']);
    let block = await web3.eth.getBlock(row['blockNumber']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    row['timestamp'] = block['timestamp'];
    const isAcceptedOffer = methodSignatures[tx.input.slice(2, 10)] == "acceptOffer";

    let event_id = `${row['returnValues']['newOwner']}-PURCHASEDTOKENFROM-${row['returnValues']['oldOwner']}-${block['timestamp']}-${row['transactionHash']}`;

    if (isAcceptedOffer) {

        // For user activity panel
        if (trackActivity) try { 
            await handlePurchaseTracking(event_id, tx, row, block, true);
        } catch (e) { console.log(e); }

        // REMOVE CURRENT BID
        let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 AND "offerHash" = $4 ORDER BY "timestamp" DESC LIMIT 1', 
            [id, row['returnValues']['newOwner'], price.toString(), tradeHash]);
        if (bid !== null) {
            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type", "chainName", "tradeHash", "seller", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)', 
                [fillId, CA, tokenNumber, id, price.toString(), block['timestamp'], row['returnValues']['newOwner'], 'bid', CHAIN_NAME, tradeHash, row['returnValues']['oldOwner'], row['transactionHash']]);

            // REMOVE BID
            await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

            // UPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            let [lowestBid, highestBid] = await getTokenPrices(id);
            await db.any('UPDATE "tokens" SET "lowestBid" = $1, "highestBid" = $2 WHERE "id" = $3', [lowestBid, highestBid, id]);

            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [CA]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            const volumeOverall = web3.utils.toBN(collection['volumeOverall']);
            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [volumeOverall.add(price).toString(), CA]);

            // UPDATE CHAIN
            let chain = await db.oneOrNone('SELECT * FROM "chains" where "name" = $1', [CHAIN_NAME]);
            if (chain === null) {
                console.log("Chain not in database");
            } else {
                const oldVolume = web3.utils.toBN(chain['volume'] ?? "0");
                const oldTrades = chain['trades'] ?? 0;
                await db.any('UPDATE "chains" SET "volume" = $1, "trades" = $2 WHERE "name" = $3', [oldVolume.add(price).toString(), Number(oldTrades) + 1, CHAIN_NAME]);
            }

            console.log(`[FILL BID] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}; from: ${row['returnValues']['newOwner']}; price: ${row['returnValues']['price']}`);
        } else {
            console.log("Bid not in database");
        }
    } else { // Listing fulfilled

        // For user activity panel
        if (trackActivity) try { 
            await handlePurchaseTracking(event_id, tx, row, block, false);
        } catch (e) { console.log(e); }

        let filledAsk = await db.oneOrNone('SELECT * FROM "askHistories" WHERE "tokenId" = $1 AND "value" = $2 AND "accepted" = $3 AND "listingHash" = $4 ORDER BY "timestamp" DESC LIMIT 1', 
            [id, price.toString(), 0, tradeHash]);
        if (filledAsk !== null) {
            const askHistoryId = filledAsk['id'];

            // UPDATE TOKEN
            let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
            if (token === null) {
                console.log("Token not in database");
            }

            await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [0, id]);

            // SAVE FILL
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type", "chainName", "tradeHash", "seller", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)', 
                [fillId, CA, tokenNumber, id, price.toString(), block['timestamp'], row['returnValues']['newOwner'], 'ask', CHAIN_NAME, tradeHash, row['returnValues']['oldOwner'], row['transactionHash']]);

            // UPDATE OLD ASK HISTORY
            await db.any('UPDATE "askHistories" SET "accepted" = $1 WHERE "id" = $2', [1, askHistoryId]);

            // REMOVE CURRENT ASK
            await db.any('DELETE FROM "asks" WHERE "id" = $1', [id]);

            // SAVE DELIST TO ASK HISTORY
            await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
            [CA, tokenNumber, id, 0, block['timestamp'], 0, row['transactionHash'], row['returnValues']['oldOwner'], CHAIN_NAME, tradeHash, 0]);
            
            // UPDATE COLLECTION
            let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [CA]);
            if (collection === null) {
                console.log("Collection not in database");
            }

            let [floorPrice, ceilingPrice] = await getCollectionPrices(CA);
            const volumeOverall = web3.utils.toBN(collection['volumeOverall']);
            await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2, "volumeOverall" = $3 WHERE "id" = $4', [floorPrice, ceilingPrice, volumeOverall.add(price).toString(), CA]);
            await db.any('UPDATE "collections" SET "volumeOverall" = $1 WHERE "id" = $2', [volumeOverall.add(price).toString(), CA]);

            // UPDATE CHAIN
            let chain = await db.oneOrNone('SELECT * FROM "chains" where "name" = $1', [CHAIN_NAME]);
            if (chain === null) {
                console.log("Chain not in database");
            } else {
                const oldVolume = web3.utils.toBN(chain['volume'] ?? "0");
                const oldTrades = chain['trades'] ?? 0;
                await db.any('UPDATE "chains" SET "volume" = $1, "trades" = $2 WHERE "name" = $3', [oldVolume.add(price).toString(), Number(oldTrades) + 1, CHAIN_NAME]);
            }

            console.log(`[FILL ASK] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}; price: ${row['returnValues']['price']}`)
        } else {
            console.log("Ask not in database");
        }
    }
}


async function handledBidPlaced(row) {
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['id'];
    const id = `${CA}-${tokenNumber}`;
    const bidId = `${CA}-${tokenNumber}-${row['returnValues']['buyer']}-${row['transactionHash']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    const expiry = web3.utils.toBN(row['returnValues']['expiry'] ?? 0);
    const from = row['returnValues']['buyer'];
    const offerHash = row['returnValues']['offerHash'] ?? "OLD_CONTRACT";
    let potentialSeller = row['returnValues']['potentialSeller'];
    if (potentialSeller === undefined) {
        try {
            const tokenContract = new web3.eth.Contract(ABIS.NFT, CA);
            potentialSeller = await tokenContract.methods.ownerOf(tokenNumber).call();
        } catch (e) {
            console.log(e);
        } finally {
            if (potentialSeller === undefined) {
                potentialSeller = "UNKNOWN";
            }
        }
    }
    let block = await web3.eth.getBlock(row['blockNumber']);
    let event_id = `${from}-OFFERPLACED-${block['timestamp']}-${row['transactionHash']}`;

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName", "tradingEnabled", "isERC1155") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
            [CA, 0, 0, 0, CHAIN_NAME, true, false]);
        collection = {
            'id': row['returnValues']['token'],
            'ceilingPrice': 0,
            'floorPrice': 0,
            'volumeOverall': 0,
            'chainName': CHAIN_NAME,
            'tradingEnabled': true
        };
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
    if (token === null) {
        await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "lowestBid", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6, $7)', 
            [id, tokenNumber, CA, 0, price.toString(), price.toString(), CHAIN_NAME]);
        token = {
            'id': id,
            'tokenNumber': tokenNumber,
            'collectionId': CA,
            'currentAsk': 0,
            'lowestBid': price,
            'highestBid': price
        };
    } else {
        if (web3.utils.toBN(token['lowestBid']).lte(price)) {
            await db.any('UPDATE "tokens" SET "lowestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['lowestBid'] = price;
        }

        if (web3.utils.toBN(token['highestBid']).gte(price)) {
            await db.any('UPDATE "tokens" SET "highestBid" = $1 WHERE "id" = $2', [price.toString(), id]);
            token['highestBid'] = price;
        }
    }

    // For user activity panel
    if (trackActivity) try { 
        await handleOfferTracking(event_id, row, block, from);
    } catch (e) { console.log(e); }

    // SAVE BID
    await db.any('INSERT INTO "bids" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "transactionHash", "expiry", "offerHash", "chainName", "seller") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)',
        [bidId, CA, tokenNumber, id, price.toString(), block['timestamp'], from, row['transactionHash'], expiry.toString(), offerHash, CHAIN_NAME, potentialSeller ]);

    console.log(`[BID PLACED] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}; price: ${price}}`)

}


async function handleBidCancelled(row) {
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    const offerer = row['returnValues']['buyer'];
    let block = await web3.eth.getBlock(row['blockNumber']);
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['id'];
    const id = `${CA}-${tokenNumber}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    const offerHash = row['returnValues']['offerHash'] ?? "OLD_CONTRACT";

    // REMOVE CURRENT BID
    let bid = await db.oneOrNone('SELECT * FROM "bids" WHERE "tokenId" = $1 AND "buyer" = $2 AND "value" = $3 AND "offerHash" = $4 ORDER BY "timestamp" ASC LIMIT 1', 
        [id, offerer, price.toString(), offerHash]);
    if (bid !== null) {
        await db.any('DELETE FROM "bids" WHERE "id" = $1', [bid['id']]);

        // UPDATE TOKEN
        let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [id]);
        if (token === null) {
            console.log("Token not in database");
        }

        // For user activity panel
        if (trackActivity) try { 
            await handleOfferCancelTracking(id, row, block, offerer)
        } catch (e) { console.log(e); }

        let [lowestBid, highestBid] = await getTokenPrices(id);
        await db.any('UPDATE "tokens" SET "lowestBid" = $1, "highestBid" = $2 WHERE "id" = $3', [lowestBid, highestBid, id]);

        console.log(`[BID CANCELLED] tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}; from: ${offerer}; price: ${row['returnValues']['price']}`);
    } else {
        console.log("Bid not in database");
    }
}

// NEW COLLECTION ADDED / ROYALTY INFO MODIFIED
async function handleCollectionModified(row) {

    const isERC1155 = row['address'] === chainObject.fungible_marketplace_contract_address;

    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [row['returnValues']['token']]);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName", "tradingEnabled", "royalty", "collectionOwner", "timestamp", "lastModifiedTimestamp", "lastModifiedTxHash", "isERC1155") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)', 
            [row['returnValues']['token'], 0, 0, 0, CHAIN_NAME, row['returnValues']['enabled'], web3.utils.toBN(row['returnValues']['collectionOwnerFee']).toString(), row['returnValues']['owner'], row['returnValues']['timestamp'], row['returnValues']['timestamp'], row['transactionHash'], isERC1155]);
            console.log(`Added new collection to database: ${row['returnValues']['token']}.`);
    } else {
        await db.any('UPDATE "collections" SET "tradingEnabled" = $1, "royalty" = $2, "collectionOwner" = $3, "lastModifiedTimestamp" = $4, "lastModifiedTxHash" = $5 WHERE "id" = $6',
            [row['returnValues']['enabled'], web3.utils.toBN(row['returnValues']['collectionOwnerFee']).toString(), row['returnValues']['owner'], row['returnValues']['timestamp'], row['transactionHash'], row['returnValues']['token']]);
            console.log(`Collection updated: ${row['returnValues']['token']}. (${row['returnValues']['enabled'] ? "TRADING" : "NOT TRADING"} | FEE: ${web3.utils.toBN(row['returnValues']['collectionOwnerFee']).toString()} | Collection owner ${row['returnValues']['owner']})`);
    }

}


async function handleTransaction(row) {

    if (row['timestamp'] === undefined) {
        let block = await web3.eth.getBlock(row['blockNumber']);
        row['timestamp'] = block['timestamp'];
    }

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
      1155's
******************/

async function handleTradeOpened(row) {
    const id = `${row['returnValues']['tradeId']}`;
    const tokenId = `${row['returnValues']['token']}-${row['returnValues']['tokenId']}`;
    const askId = `${tokenId}-${row['transactionHash']}-${id}}}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    const quantity = web3.utils.toBN(row['returnValues']['quantity']);
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['tokenId'];
    const maker = row['returnValues']['maker'];
    const tradeFlags = row['returnValues']['tradeFlags'];
    const tradeType = tradeFlags?.[0] === "0" ? 'BUY' : 'SELL';
    const allowPartialFills = tradeFlags?.[1];
    const isEscrowed = tradeFlags?.[2];
    const expiration = web3.utils.toBN(row['returnValues']['expiry']);
    const timestamp = row['returnValues']['timestamp'];
    let block = await web3.eth.getBlock(row['blockNumber']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let event_id = `${tx['from']}-TRADEOPENED-${timestamp}-${row['transactionHash']}`;

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
            [event_id, tx['from'], "TRADE_OPENED", CHAIN_NAME, CA, tokenNumber, price.mul(quantity).toString(), timestamp, id, row['transactionHash']]
        ); 

        //Logic for 1155 trade tracking/scoring
        let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [tx['from']]);
        if (trader === null && tradeType === "BUY") {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "offerCount", "offerVolume") VALUES ($1, $2, $3)`, [tx['from'], 1, price.mul(quantity).toString()])
        } else if (trader === null && tradeType === "SELL") {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "listingCount") VALUES ($1, $2)`, [tx['from'], 1])
        } else if (trader !== null && tradeType === "BUY") {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "offerCount" = $1, "offerVolume" = $2 WHERE "userAddress" = $3`, [Number(trader?.['offerCount'] ?? 0) + 1, web3.utils.toBN(trader?.['offerVolume'] ?? 0).add(price.mul(quantity)).toString(), tx['from']]);
        } else if (trader !== null && tradeType === "SELL"){
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "listingCount" = $1 WHERE "userAddress" = $2`, [Number(trader?.['listingCount'] ?? 0) + 1, tx['from']]);
        }
    } catch (e) { console.log(e); }

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', CA);
    if (collection === null) {
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName", "tradingEnabled", "isERC1155") VALUES ($1, $2, $3, $4, $5, $6, $7)', [CA, 0, 0, 0, CHAIN_NAME, true, true]);
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [tokenId]);
    if (token === null) { // new token
        if (tradeType === "SELL") {
            await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', 
                [tokenId, tokenNumber, CA, price.toString(), 0, CHAIN_NAME]);
        } else {
            await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', 
                [tokenId, tokenNumber, CA, 0, price.toString(), CHAIN_NAME]);
        }
    } else {
        if (tradeType === "SELL" && price.lte(web3.utils.toBN(token?.['currentAsk']))) {
            await db.any('UPDATE "tokens" SET "currentAsk" = $1 WHERE "id" = $2', [price.toString(), tokenId]);
        } else if (tradeType === "BUY" && price.gte(web3.utils.toBN(token?.['highestBid']))) {
            await db.any('UPDATE "tokens" SET "highestBid" = $1 WHERE "id" = $2', [price.toString(), tokenId]);
        }
    }

    // UPDATE COLLECTION FLOOR/CEILINGS
    if (tradeType === "SELL" && web3.utils.toBN(collection['ceilingPrice']).lte(price)) {
        await db.any('UPDATE "collections" SET "ceilingPrice" = $1 WHERE "id" = $2', [price.toString(), CA]);
    }

    if (tradeType === "SELL" && web3.utils.toBN(collection['floorPrice']).gte(price)) {
        await db.any('UPDATE "collections" SET "floorPrice" = $1 WHERE "id" = $2', [price.toString(), CA]);
    }

    // SAVE NEW TRADE
    await db.any('DELETE FROM "fungibleTrades" WHERE "tradeHash" = $1', [id]);
    await db.any('INSERT INTO "fungibleTrades" ("tradeHash", "contractAddress", "tokenNumber", "status", "tradeType", "allowPartials", "isEscrowed", "totalQuantity", "remainingQuantity", "pricePerUnit", "openedTimestamp", "lastUpdatedTimestamp", "chainName", "maker", "expiry", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)', 
        [id, CA, tokenNumber, 'OPEN', tradeType, allowPartialFills, isEscrowed, quantity.toString(), quantity.toString(), price.toString(), timestamp, timestamp, CHAIN_NAME, maker, expiration.toString(), row['transactionHash']]);

    //INSERT INTO ASKHISTORIES
    if (tradeType === "SELL") {
        await db.any('INSERT INTO "askHistories" ("collectionId", "tokenNumber", "tokenId", "value", "timestamp", "accepted", "transactionHash", "lister", "chainName", "listingHash", "expiry", "quantity") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)',
        [CA, tokenNumber, tokenId, price.toString(), timestamp, 0, row['transactionHash'], maker, CHAIN_NAME, id, expiration.toString(), quantity.toString()]
    );
    }

    console.log(`[1155 TRADE OPENED] tradeId: ${id}; tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}}; price: ${price}; quantity: ${quantity}`);
}


async function handleTradeCancelled(row) {
    const id = `${row['returnValues']['tradeId']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);
    const quantity = web3.utils.toBN(row['returnValues']['quantity']);
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['tokenId'];
    const tokenId = `${CA}-${tokenNumber}`;
    const maker = row['returnValues']['maker'];
    const tradeFlags = row['returnValues']['tradeFlags'];
    const tradeType = tradeFlags?.[0] === "0" ? 'BUY' : 'SELL';
    const allowPartialFills = tradeFlags?.[1];
    const isEscrowed = tradeFlags?.[2];
    const timestamp = row['returnValues']['timestamp'];
    const expiration = web3.utils.toBN(row['returnValues']['expiry']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    let event_id = `${tx['from']}-TRADECANCELLED-${timestamp}-${row['transactionHash']}`;

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
            [event_id, tx['from'], "TRADE_CANCELLED", CHAIN_NAME, CA, tokenNumber, price.mul(quantity).toString(), timestamp, maker, id, row['transactionHash']]
        ); 

        //Logic for 1155 trade tracking/scoring
        let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [tx['from']]);
        if (trader === null && tradeType === "BUY") {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "offerCount", "offerVolume") VALUES ($1, $2, $3)`, [tx['from'], 0, 0])
        } else if (trader === null && tradeType === "SELL") {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "listingCount") VALUES ($1, $2)`, [tx['from'], 0])
        } else if (trader !== null && tradeType === "BUY") {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "offerCount" = $1, "offerVolume" = $2 WHERE "userAddress" = $3`, [Number(trader?.['offerCount'] ?? 0) - 1, web3.utils.toBN(trader?.['offerVolume'] ?? 0).sub(price.mul(quantity)).toString(), tx['from']]);
        } else if (trader !== null && tradeType === "SELL"){
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "listingCount" = $1 WHERE "userAddress" = $2`, [Number(trader?.['listingCount'] ?? 0) - 1, tx['from']]);
        }
    } catch (e) { console.log(e); }

    // CREATE OR GET COLLECTION
    let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', CA);
    if (collection === null) { // new collection - should be impossible
        await db.any('INSERT INTO "collections" ("id", "ceilingPrice", "floorPrice", "volumeOverall", "chainName", "tradingEnabled", "isERC1155") VALUES ($1, $2, $3, $4, $5, $6, $7)', [CA, 0, 0, 0, CHAIN_NAME, true, true]);
    }

    // SAVE TRADE CANCELLATION
    const trade = await db.oneOrNone('SELECT * FROM "fungibleTrades" where "tradeHash" = $1', [id]);
    if (trade === null) { //should be impossible
        await db.any('INSERT INTO "fungibleTrades" ("tradeHash", "contractAddress", "tokenNumber", "status", "tradeType", "allowPartials", "isEscrowed", "totalQuantity", "remainingQuantity", "pricePerUnit", "openedTimestamp", "lastUpdatedTimestamp", "chainName", "maker", "expiry", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)', 
        [id, CA, tokenNumber, 'CANCELLED', tradeType, allowPartialFills, isEscrowed, quantity.toString(), quantity.toString(), price.toString(), timestamp, timestamp, CHAIN_NAME, maker, expiration.toString(), row['transactionHash']]);
    } else {
        await db.any('UPDATE "fungibleTrades" SET "status" = $1, "lastUpdatedTimestamp" = $2 WHERE "tradeHash" = $3', ['CANCELLED', timestamp, id]);
    }

    // SAVE OR UPDATE TOKEN
    let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [tokenId]);
    if (token === null) { // new token - should be impossible
        if (tradeType === "SELL") {
            await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', 
                [tokenId, tokenNumber, CA, 0, 0, CHAIN_NAME]);
        } else {
            await db.any('INSERT INTO "tokens" ("id", "tokenNumber", "collectionId", "currentAsk", "highestBid", "chainName") VALUES ($1, $2, $3, $4, $5, $6)', 
                [tokenId, tokenNumber, CA, 0, 0, CHAIN_NAME]);
        }
    } else {
        const [highestBid, lowestAsk] = await getFungibleTokenPrices(CA, tokenNumber);
        await db.any('UPDATE "tokens" SET "currentAsk" = $1, "highestBid" = $2 WHERE "id" = $3', [lowestAsk, highestBid, tokenId]);
    }
    
    // UPDATE COLLECTION FLOOR/CEILINGS
    const [floorPrice, ceilingPrice] = await getFungibleCollectionPrices(CA);
    await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2 WHERE "id" = $3', [floorPrice, ceilingPrice, CA]);

    console.log(`[1155 TRADE CANCELLED] tradeId: ${id}; tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}}; price: ${price}; quantity: ${quantity}`);

}


async function handleTradeAccepted(row) {
    const id = `${row['returnValues']['tradeId']}`;
    const price = web3.utils.toBN(row['returnValues']['price']);    
    const CA = row['returnValues']['token'];
    const tokenNumber = row['returnValues']['tokenId'];
    const tokenId = `${CA}-${tokenNumber}`;
    const fillId = `${tokenId}-${row['transactionHash']}-${id}}}`;
    const tradeType = row['returnValues']['tradeType'] === "0" ? 'BUY' : 'SELL';
    const timestamp = row['returnValues']['timestamp'];
    const expiration = web3.utils.toBN(row['returnValues']['expiry']);
    let tx = await web3.eth.getTransaction(row['transactionHash']);
    const buyer = row['returnValues']['newOwner'];
    const seller = row['returnValues']['oldOwner'];
    let event_id = `${tx['from']}-TRADEACCEPTED-${timestamp}-${row['transactionHash']}`;
    // const quantity = web3.utils.toBN(row['returnValues']['quantity']); // the event returns the wrong value, rip. get it the hard way instead

    const function_inputs = web3.eth.abi.decodeParameters(['bytes32', 'uint256'], tx["input"].slice(10));
    const quantity = web3.utils.toBN(function_inputs?.[1]);

    // For user activity panel
    if (trackActivity) try { 
        await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
            [event_id, tx['from'], "TRADE_ACCEPTED", CHAIN_NAME, CA, tokenNumber, price.mul(quantity).toString(), timestamp, id, row['transactionHash']]
        ); 

        //TODO - Logic for 1155 trade tracking/scoring. IMPROVE ME, THIS IS BASIC
        let traderBuyer = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [buyer]);
        let traderSeller = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [seller]);
        if (traderBuyer === null) {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "purchaseCount", "purchaseVolume") VALUES ($1, $2, $3)`, [buyer, 1, price.mul(quantity).toString()]);
        } else {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "purchaseCount" = $1, "purchaseVolume" = $2 WHERE "userAddress" = $3`, [Number(traderBuyer?.["purchaseCount"] ?? 0) + 1, web3.utils.toBN(traderBuyer?.["purchaseVolume"] ?? 0).add(web3.utils.toBN(price.mul(quantity))).toString(), tx['from']]);
        }
        if (traderSeller === null) {
            await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "saleCount", "saleVolume") VALUES ($1, $2, $3)`, [seller, 1, price.mul(quantity).toString()]);
        } else {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "saleCount" = $1, "saleVolume" = $2 WHERE "userAddress" = $3`, [Number(traderSeller?.["saleCount"] ?? 0) + 1, web3.utils.toBN(traderSeller?.["saleVolume"] ?? 0).add(web3.utils.toBN(price.mul(quantity))).toString(), tx['from']]);
        }
    } catch (e) { console.log(e); }

    try { // wrapping all this just in case 
        let trade = await db.any('SELECT * FROM "fungibleTrades" WHERE "tradeHash" = $1', [id]);
        if (trade === null) {
            console.log(`Trade [${id}] not in database`);
        } else {
            //First, update the fungible trade table with the new status
            const remainingQuantity = web3.utils.toBN(trade?.[0]?.["remainingQuantity"] ?? 0).sub(quantity);
            const newStatus = remainingQuantity.gt(web3.utils.toBN(0)) ? 'PARTIAL' : 'ACCEPTED';
            await db.any('UPDATE "fungibleTrades" SET "status" = $1, "remainingQuantity" = $2, "lastUpdatedTimestamp" = $3 WHERE "tradeHash" = $4', [newStatus, remainingQuantity.toString(), timestamp, id]);

            //Now update the regular fills table with the the amount sold
            await db.any('INSERT INTO "fills" ("id", "collectionId", "tokenNumber", "tokenId", "value", "timestamp", "buyer", "type", "chainName", "tradeHash", "seller", "transactionHash", "isERC1155", "quantity") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)', 
                [fillId, CA, tokenNumber, tokenId, price.toString(), timestamp, buyer, tradeType, CHAIN_NAME, id, seller, row['transactionHash'], true, quantity.toString()]);

            //We only need to update price info if the trade is now closed / accepted.
            if (newStatus === "ACCEPTED") {

                // Get and update token price data
                let token = await db.oneOrNone('SELECT * FROM "tokens" WHERE "id" = $1', [tokenId]);
                if (token === null) {
                    console.log("Token not in database");
                } else {
                    const [highestBid, lowestAsk] = await getFungibleTokenPrices(CA, tokenNumber);
                    await db.any('UPDATE "tokens" SET "currentAsk" = $1, "highestBid" = $2 WHERE "id" = $3', [lowestAsk, highestBid, tokenId]);
                }

                // Get and update collection price data
                let collection = await db.oneOrNone('SELECT * FROM "collections" WHERE "id" = $1', [CA]);
                if (collection === null) {
                    console.log("Collection not in database");
                } else {
                    const [floorPrice, ceilingPrice] = await getFungibleCollectionPrices(CA);
                    const newVolume = web3.utils.toBN(collection['volumeOverall']).add(price.mul(quantity)).toString();
                    await db.any('UPDATE "collections" SET "floorPrice" = $1, "ceilingPrice" = $2, "volumeOverall" = $3 WHERE "id" = $4', [floorPrice, ceilingPrice, newVolume, CA]);
                }
            }

            console.log(`[1155 TRADE ACCEPTED] tradeId: ${id}; tx: ${row['transactionHash']}; token: ${tokenNumber}; collection: ${CA}}; price: ${price.toString()}; quantity: ${quantity.toString()}`);
        }
    } catch (e) {
        console.log("Error occurred updating/accepting an open trade.", e);
    }
}


async function handleWinnerSet(row) {
    const gameId = `${row['returnValues']['gameID']}`;
    const winner = `${row['returnValues']['winner']}`;
    const timestamp = `${row['returnValues']['setTime']}`;
    const txHash = `${row['transactionHash']}`;

    try {
        let game = await db.oneOrNone('SELECT * FROM "nllGames" WHERE "gameId" = $1', [gameId]);
        if (game === null) {
            await db.any('INSERT INTO "nllGames" ("gameId", "winnerAddress", "timestamp", "transactionHash") VALUES ($1, $2, $3, $4)', [gameId, winner, timestamp, txHash]);
        } else {
            await db.any('UPDATE "nllGames" SET "winnerAddress" = $1, "timestamp" = $2, "transactionHash" = $3 WHERE "gameId" = $4', [winner, timestamp, txHash, gameId]);
        }
    } catch {
        console.log(`Failed to index WinnerSet for this game: [${gameId}, ${winner}, ${timestamp}, ${txHash}]`);
    }
    
}


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

async function getFungibleTokenPrices(CA, tokenId) {
    let highestBid = null;
    let lowestAsk = null;

    let trades = await db.manyOrNone(`SELECT * FROM "fungibleTrades" WHERE "contractAddress" = $1 AND "tokenNumber" = $2 AND ("status" = 'OPEN' OR "status" = 'PARTIAL')`, [CA, tokenId]);
    if (trades.length > 0) {
        for (let trade of trades) {
            if (trade['tradeType'] === "BUY" && (highestBid === null || (web3.utils.toBN(trade['pricePerUnit'])).gte(web3.utils.toBN(highestBid)))) {
                highestBid = web3.utils.toBN(trade['pricePerUnit']);
            }
            if (trade['tradeType'] === "SELL" && (lowestAsk === null || (web3.utils.toBN(trade['pricePerUnit'])).lte(web3.utils.toBN(lowestAsk)))) {
                lowestAsk = web3.utils.toBN(trade['pricePerUnit']);
            }
        }
    }

    if (highestBid === null) {
        highestBid = web3.utils.toBN(0);
    }
    if (lowestAsk === null) {
        lowestAsk = web3.utils.toBN(0);
    }

    return [highestBid.toString(), lowestAsk.toString()];
}

async function getFungibleCollectionPrices(collectionId) {
    let floorPrice = null;
    let ceilingPrice = null;

    let trades = await db.manyOrNone(`SELECT * FROM "fungibleTrades" WHERE "contractAddress" = $1 AND ("status" = 'OPEN' OR "status" = 'PARTIAL') AND "tradeType" = 'SELL'`, [collectionId]);
    if (trades.length > 0) {
        for (let trade of trades) {
            if (floorPrice === null || (web3.utils.toBN(trade['pricePerUnit'])).lte(web3.utils.toBN(floorPrice))) {
                floorPrice = web3.utils.toBN(trade['pricePerUnit']);
            }
            if (ceilingPrice === null || (web3.utils.toBN(trade['pricePerUnit'])).gte(web3.utils.toBN(ceilingPrice))) {
                ceilingPrice = web3.utils.toBN(trade['pricePerUnit']);
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


/*****************
     TRACKING
******************/
async function handleListingTracking(event_id, tx, row, price, block) {
    //regular tracking
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [event_id, tx['from'], "TOKEN_LISTING", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], price.toString(), block['timestamp'], row['returnValues']['listingHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //score tracking
    let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [tx['from']]);
    if (trader === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "listingCount") VALUES ($1, $2)`, [tx['from'], 1])
    } else {
        await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "listingCount" = $1 WHERE "userAddress" = $2`, [Number(trader?.['listingCount'] ?? 0) + 1, tx['from']]);
    }
}


async function handleDelistingTracking(event_id, tx, row, block) {
    //regular tracking
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [event_id, tx['from'], "TOKEN_DELISTING", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], 0, block['timestamp'], row['returnValues']['listingHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //score tracking
    let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [tx['from']]);
    if (trader === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "listingCount") VALUES ($1, $2)`, [tx['from'], 0])
    } else {
        await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "listingCount" = $1 WHERE "userAddress" = $2`, [Number(trader?.['listingCount'] ?? 0) - 1, tx['from']]);
    }
}


async function handlePurchaseTracking(event_id, tx, row, block, isOffer) {
    //track purchaser
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [event_id + '-buyer', row['returnValues']['newOwner'], isOffer ? 'TOKEN_PURCHASED_OFFER' : 'TOKEN_PURCHASED_LISTING', CHAIN_NAME, row['returnValues']['collection'], row['returnValues']['tokenId'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['tradeHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //track sale
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [event_id + '-seller', row['returnValues']['oldOwner'], isOffer ? 'TOKEN_SOLD_OFFER' : 'TOKEN_SOLD_LISTING', CHAIN_NAME, row['returnValues']['collection'], row['returnValues']['tokenId'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['tradeHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //score tracking
    let seller = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [row['returnValues']['oldOwner']]);
    let purchaser = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [row['returnValues']['newOwner']]);
    if (seller === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "saleCount", "saleVolume") VALUES ($1, $2, $3)`, [row['returnValues']['oldOwner'], 1, web3.utils.toBN(row['returnValues']['price']).toString()])
    } else {
        await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "saleCount" = $1, "saleVolume" = $2 WHERE "userAddress" = $3`, [Number(seller?.['saleCount'] ?? 0) + 1, web3.utils.toBN(seller?.['saleVolume'] ?? 0).add(web3.utils.toBN(row['returnValues']['price'])).toString(), row['returnValues']['oldOwner']]);
    }

    if (purchaser === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "purchaseCount", "purchaseVolume") VALUES ($1, $2, $3)`, [tx['from'], 1, web3.utils.toBN(row['returnValues']['price']).toString()])
    } else {
        if (isOffer) {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "purchaseCount" = $1, "purchaseVolume" = $2, "offerVolume" = $3 WHERE "userAddress" = $4`, [Number(purchaser?.['purchaseCount'] ?? 0) + 1, web3.utils.toBN(purchaser?.['purchaseVolume'] ?? 0).add(web3.utils.toBN(row['returnValues']['price'])).toString(), web3.utils.toBN(purchaser?.['offerVolume'] ?? 0).sub(web3.utils.toBN(row['returnValues']['price'])).toString(), row['returnValues']['newOwner']]);
        } else {
            await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "purchaseCount" = $1, "purchaseVolume" = $2 WHERE "userAddress" = $3`, [Number(purchaser?.['purchaseCount'] ?? 0) + 1, web3.utils.toBN(purchaser?.['purchaseVolume'] ?? 0).add(web3.utils.toBN(row['returnValues']['price'])).toString(), row['returnValues']['newOwner']]);
        }
    }
}


async function handleOfferTracking(event_id, row, block, from) {
    //regular tracking
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [event_id, from, "OFFER_PLACED", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['offerHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //track score
    let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [from]);
    if (trader === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "offerCount", "offerVolume") VALUES ($1, $2, $3)`, [from, 1, web3.utils.toBN(row['returnValues']['price']).toString()])
    } else {
        await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "offerCount" = $1, "offerVolume" = $2 WHERE "userAddress" = $3`, [Number(trader?.['offerCount'] ?? 0) + 1, web3.utils.toBN(trader?.['offerVolume'] ?? 0).add(web3.utils.toBN(row['returnValues']['price'])).toString(), from]);
    }
}


async function handleOfferCancelTracking(id, row, block, offerer) {
    //regular tracking
    await db.any('INSERT INTO "activityHistories" ("eventId", "userAddress", "activity", "chainName", "tokenAddress", "tokenNumber", "amount", "timestamp", "tradeHash", "transactionHash") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)', 
        [id, offerer, "OFFER_CANCELLED", CHAIN_NAME, row['returnValues']['token'], row['returnValues']['id'], web3.utils.toBN(row['returnValues']['price']).toString(), block['timestamp'], row['returnValues']['offerHash'] ?? "OLD_CONTRACT", row['transactionHash']]
    ); 

    //track score
    let trader = await db.oneOrNone(`SELECT * FROM "${CHAIN_NAME}Traders" WHERE "userAddress" = $1`, [offerer]);
    if (trader === null) {
        await db.any(`INSERT INTO "${CHAIN_NAME}Traders" ("userAddress", "offerCount", "offerVolume") VALUES ($1, $2, $3)`, [offerer, 0, 0])
    } else {
        await db.any(`UPDATE "${CHAIN_NAME}Traders" SET "offerCount" = $1, "offerVolume" = $2 WHERE "userAddress" = $3`, [Number(trader?.['offerCount'] ?? 0) - 1, web3.utils.toBN(trader?.['offerVolume'] ?? 0).sub(web3.utils.toBN(row['returnValues']['price'])).toString(), offerer]);
    }
}


async function setupChain(chainObject) {
    // UPDATE/PULL CHAIN DATA IN DB
    let chain = await db.oneOrNone('SELECT * FROM "chains" WHERE "name" = $1', [CHAIN_NAME]);
    if (chain === null) {
        await db.any('INSERT INTO "chains" ("name", "chainId", "startBlock", "lastIndexedBlock") VALUES ($1, $2, $3, $4)', 
            [CHAIN_NAME, chainObject?.chain_id, chainObject?.startBlock ?? 0, chainObject?.startBlock ?? 0]);
    } else {
        chainObject.startBlock = parseInt(chain['lastIndexedBlock']);
    }
    return chainObject;
}