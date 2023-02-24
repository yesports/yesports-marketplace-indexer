const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const fs = require("fs").promises;
const http = require("http");
const { postgraphile } = require("postgraphile");
const pgp = require("pg-promise")({});
const cn = 'postgres://postgres:<DBPASS>@<DBHOST>:5432/<DBNAME>?ssl=true';
const db = pgp(cn);

//Health Check & Convenience Endpoints
http.createServer(async function (req, res) {
    res.writeHead(200, {'Content-Type': 'application/json'});

    let responseData = {};

    let lastBlocksQuery = await db.manyOrNone('SELECT name, value, timestamp FROM "meta"');
    if (lastBlocksQuery.length > 0) {
        for (let row of lastBlocksQuery) {
            responseData[row['name']] = row['value'];
        }
    }

    responseData['last_block_polygon'] = parseInt(await fs.readFile("./last_block_polygon.txt"));

    res.write(JSON.stringify(responseData));
    res.end();
}).listen(8080);




//GraphQL server
http.createServer(
    postgraphile(
        cn,
        "public",
        {
            watchPg: false,
            graphiql: true,
            enhanceGraphiql: true,
            appendPlugins: [ConnectionFilterPlugin],
            enableCors: true,
            disableDefaultMutations: true,
        }
    )
).listen(process.env.PORT || 3000);