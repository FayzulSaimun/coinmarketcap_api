import asyncio
import aiohttp
import time
import pandas as pd
import datetime

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


df = pd.read_csv("coinmarket_coins.csv")
ids = df["id"].values.tolist()
slugs = df["slug"].values.tolist()

results = []
start = time.time()


data_range = "1m"  # <-- change the data range here


def get_tasks_one(session):
    tasks = []
    for i in ids:
        url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/holders/count?id={i}&range={data_range}"
        tasks.append(
            asyncio.create_task(
                session.get(
                    url,
                    proxy="http://jfbvbogu-rotate:npcjftw332f6@p.webshare.io:80/",
                    timeout=40,
                )
            )
        )
    return tasks


def get_tasks_two(session):
    tasks = []
    for i in ids:
        url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/holders/ratio?id={i}&range={data_range}"
        tasks.append(
            asyncio.create_task(
                session.get(
                    url,
                    proxy="http://jfbvbogu-rotate:npcjftw332f6@p.webshare.io:80/",
                    timeout=40,
                )
            )
        )
    return tasks


async def get_wallet_data():
    j = 0
    wallet_data = pd.DataFrame()
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        tasks = get_tasks_one(session)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            print(await response.text())
            r = await response.json()
            t = r["data"]["points"]
            if len(t) != 0:
                print("Getting Wallet Data For:", slugs[j])
                d = pd.DataFrame(t, index=[0]).T
                d.reset_index(inplace=True)
                d["slug"] = slugs[j]
                wallet_data = wallet_data.append(d)
            else:
                empty_row = {
                    "index": datetime.datetime.now(),
                    0: "No Data",
                    "slug": slugs[j],
                }
                wallet_data = wallet_data.append(empty_row, ignore_index=True)
                print("No values For:", slugs[j])

            j += 1
    return wallet_data


async def get_ratio_data():
    j = 0
    ratio_data = pd.DataFrame()
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        tasks = get_tasks_two(session)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            r = await response.json()
            t = r["data"]["points"]
            if len(t) != 0:
                print("Getting Wallet Data For:", slugs[j])
                d = pd.DataFrame(t).T
                d.reset_index(inplace=True)
                d["slug"] = slugs[j]
                ratio_data = ratio_data.append(d)
            else:
                empty_row = {
                    "index": datetime.datetime.now(),
                    "topTenHolderRatio": "No Data",
                    "topTwentyHolderRatio": "No Data",
                    "topFiftyHolderRatio": "No Data",
                    "topHundredHolderRatio": "No Data",
                    "slug": slugs[j],
                }
                ratio_data = ratio_data.append(empty_row, ignore_index=True)
                print("No values For:", slugs[j])

            j += 1
    return ratio_data


def main():
    wallet_data = asyncio.run(get_wallet_data())
    # time.sleep(20)  # <-- wait for some time to start the sceond url
    ratio_data = asyncio.run(get_ratio_data())

    finalDF = ratio_data
    finalDF.insert(1, "Wallet", wallet_data[0])
    finalDF.sort_values(by="slug", inplace=True)
    finalDF.rename(
        columns={
            "index": "Date",
            "Wallet": "Total Address",
            "topTenHolderRatio": "top_10",
            "topTwentyHolderRatio": "top_20",
            "topFiftyHolderRatio": "top_50",
            "topHundredHolderRatio": "top_100",
        },
        inplace=True,
    )
    print(finalDF.head())
    finalDF.to_csv("coinmarket_holders.csv", index=False)
    end = time.time()
    total_time = end - start

    print(
        "It took {} seconds to make {} API calls".format(
            total_time, len(ids) + len(ids)
        )
    )
    print("You did it!")
    return finalDF


try:
    main()
except aiohttp.client_exceptions.ContentTypeError:
    main()
