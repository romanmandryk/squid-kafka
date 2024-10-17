import { getFullnodeUrl, PaginatedTransactionResponse, SuiClient } from '@mysten/sui/client';
import { GraphQLClient } from 'graphql-request';
import { getSdk } from './generated/graphql';

const rpcUrl = getFullnodeUrl('mainnet');
const SUI_MAINNET_GRAPHQL_URL = 'https://sui-mainnet.mystenlabs.com/graphql'

const graphqlClient = new GraphQLClient(SUI_MAINNET_GRAPHQL_URL);
const sdk = getSdk(graphqlClient);

const MAX_REQUESTS = 1000;

async function fetchSuiTransactions(startEpoch: number) {
    let currentEpoch = startEpoch;
    let requestCount = 0;
    let fullySynced = false;
    // Outer loop for epochs
    while ( requestCount < MAX_REQUESTS && !fullySynced) {
        let cursor: string | null | undefined = null;
        let hasNextPage = true;
        let page: number = 0;
        
        // Inner loop for pages within an epoch
        while (hasNextPage && requestCount < MAX_REQUESTS) {            
            try {
                page++;
                const data = await sdk.getMoveTransfers({ epochId: currentEpoch, cursor });
                requestCount++;
                
                if (!data.epoch) {
                    console.log(`No data for epoch ${currentEpoch}. Fully synced.`);
                    fullySynced = true;
                    break; // Move to next epoch
                }

                const transactions = data.epoch.transactionBlocks.nodes;
                console.log(`Fetched ${transactions.length} transactions for epoch ${currentEpoch} page ${page}`);

                // Process transactions here
                // ...

                hasNextPage = data.epoch.transactionBlocks.pageInfo.hasNextPage;
                cursor = data.epoch.transactionBlocks.pageInfo.endCursor;

                // Optional: Add a small delay to avoid overwhelming the API
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                console.error(`Error fetching data for epoch ${currentEpoch}:`, error);
                hasNextPage = false; // Exit inner loop on error
            }
        }

        currentEpoch++;

        if (requestCount >= MAX_REQUESTS) {
            console.log(`Reached maximum number of requests (${MAX_REQUESTS}). Halting.`);
            break;
        }
    }
}

// Start fetching from epoch 550
fetchSuiTransactions(550);
