# events-api-redis-to-db

> This repository is archived, this logic was rewritten and is now a part of [inevents](https://github.com/INTEARnear/inevents).

---

This little binary listens to redis streams of all events ([nft-indexer](https://github.com/INTEARnear/nft-indexer), [potlock-indexer](https://github.com/INTEARnear/potlock-indexer), [trade-indexer](https://github.com/INTEARnear/trade-indexer), and others) and pushes it to TimescaleDB for further retrieval via [events-api-http-server](https://github.com/INTEARnear/events-api-http-server).

