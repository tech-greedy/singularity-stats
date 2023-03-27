import pg from 'pg';
import dotenv from 'dotenv';
import QueryStream from 'pg-query-stream';
dotenv.config();

const singularityDbConfig: pg.ClientConfig = {
  user: process.env.SINGULARITYMETRICS_PG_USER,
  database: process.env.SINGULARITYMETRICS_PG_DATABASE,
  password: process.env.SINGULARITYMETRICS_PG_PASSWORD,
  host: process.env.SINGULARITYMETRICS_PG_HOST,
}
const stateMarketDealsDbConfig : pg.ClientConfig = {
    user: process.env.STATEMARKETDEALS_PG_USER,
    database: process.env.STATEMARKETDEALS_PG_DATABASE,
    password: process.env.STATEMARKETDEALS_PG_PASSWORD,
    host: process.env.STATEMARKETDEALS_PG_HOST,
}

const singularityClient = new pg.Client(singularityDbConfig);
const stateMarketDealsClient = new pg.Client(stateMarketDealsDbConfig);

console.log('Connecting to Singularity DB...')
await singularityClient.connect();
console.log('Connecting to StateMarketDeals DB...')
await stateMarketDealsClient.connect();

// Get all pieces prepared by singularity
const result = await singularityClient.query({
  text: 'select distinct values[\'pieceCid\'], values[\'pieceSize\'] from events where type = \'generation_complete\'',
  rowMode: 'array',
});
console.log(`Found ${result.rows.length} pieces.`);
const pieceMap = new Map<string, number>();
for (const row of result.rows) {
    const pieceCid = row[0];
    const pieceSize = row[1];
    pieceMap.set(pieceCid, pieceSize);
}

// Compare with all pieces with deals in StateMarketDeals
console.log('Querying StateMarketDeals DB... for all deals')
const deals = stateMarketDealsClient.query(new QueryStream(
  'select piece_cid, verified_deal, sector_start_epoch from current_state',
    [],
  {
    rowMode: 'array',
    batchSize: 100_000,
  }
))

let totalSize: bigint = 0n;
let count: number = 0;

deals.on('data', (row: any[]) => {
  const cid = row[0];
  const verified = row[1];
  const sectorStartEpoch = row[2];
  count++;
  if (count % 100_000 === 0) {
    console.log(`Processed ${count} pieces with deals.`);
  }
  if (sectorStartEpoch < 0) {
    return;
  }
  if (pieceMap.has(cid)) {
    let size = pieceMap.get(cid)!;
    if (verified) {
      totalSize += BigInt(size) * 10n;
    } else {
      totalSize += BigInt(size);
    }
  }
})

await new Promise((resolve, reject) => {
    deals.on('error', reject)
    deals.on('end', resolve)
})

console.log(`Total size of QAP with deals: ${totalSize} bytes`);
console.log(` This is equivalent to ${Number(totalSize)/ 1024 / 1024 / 1024 / 1024 / 1024 / 1024} EiB`);

console.log('Closing Singularity DB...')
await singularityClient.end()
console.log('Closing StateMarketDeals DB...')
await stateMarketDealsClient.end()
