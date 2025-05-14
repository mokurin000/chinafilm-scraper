import asyncio
from dataclasses import dataclass

import polars as pl
from diskcache import Cache
from bs4 import BeautifulSoup, Tag
from aiohttp import ClientSession
from loguru import logger

# global cache for film description
FILM_CACHE = Cache("temp")


@dataclass
class Film:
    release_year: str
    film_name: str
    director: str
    publish_company: str
    description: str
    registration_place: str


def extract_sub_page(soup: BeautifulSoup) -> list[str]:
    def atag_to_href(atag: Tag) -> str:
        return atag["href"]

    return list(map(atag_to_href, soup.select("li > a.m2r_a")))


async def get_description(session: ClientSession, url: str) -> str:
    cache = FILM_CACHE.get(url)

    # if found cache (not empty), return from cache
    if cache is not None:
        return cache

    async with session.get(url) as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")
    description: str = soup.select_one("tr:nth-child(8) > td:nth-child(2)").text
    description = description.strip()[3:]

    FILM_CACHE.add(url, description)
    return description


async def extract_page(session: ClientSession, url: str) -> list[Film]:
    logger.info(f"start scraping for {url}")

    films: list[Film] = []

    # extract release year by slicing url
    release_year = url.split("/")[1][:4]

    async with session.get(url) as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")

    # `:not(:first-child)` filters out header rows.
    film_records = soup.select("tr:not(:first-child)")
    for film_record in film_records:
        detail_link = film_record.select_one("td:nth-child(2) > a")["href"]
        film_name: str = film_record.select_one("td:nth-child(3)").text
        film_name = film_name.strip()
        publish_company: str = film_record.select_one("td:nth-child(4) > script").text
        publish_company = publish_company.split("'")[1]

        director_tag: Tag = film_record.select_one("td:nth-child(5) > script")
        if director_tag is not None:
            director: str = director_tag.text
            director = director.split("'")[1]
        else:
            logger.warning(f"{film_name} in {url}: director not found")
            director = ""

        registration_place: str = film_record.select_one("td:last-child").text

        logger.info(f"fetching description for film {film_name}")
        description = await get_description(session, detail_link)

        film = Film(
            description=description,
            release_year=release_year,
            film_name=film_name,
            publish_company=publish_company.strip(),
            registration_place=registration_place.strip(),
            director=director,
        )
        films.append(film)

    return films


async def extract_page_url(session: ClientSession, page_num: int) -> list[str]:
    async with session.get(f"index_{page_num}.html") as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")
    return extract_sub_page(soup)


# scrape initial film pages
async def scrape(session: ClientSession) -> list[str]:
    pages = []
    async with session.get("index.html") as resp:
        document = await resp.text()
        soup = BeautifulSoup(document, features="lxml")

    pages.extend(extract_sub_page(soup))

    page_count = int(
        next(line for line in document.split("\n") if "countPage" in line)
        .split("var countPage = ")[1]
        .split()[0]
    )

    # extract announcement URLs
    for announcements in await asyncio.gather(
        *(extract_page_url(session, page_num) for page_num in range(1, page_count))
    ):
        pages.extend(announcements)

    return pages


async def main():
    # initialize http session once, for the entire networking lifetime
    async with ClientSession(
        base_url="https://www.chinafilm.gov.cn/xxgk/gsxx/dybalx/",
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0"
        },
    ) as session:
        pages = await scrape(session)
        films = []

        try:
            for page in pages:
                films.extend(await extract_page(session, page))
        except Exception as e:
            logger.error(e)
        except:  # noqa: E722, bare `except` for Ctrl-C during asyncio waiter
            logger.info("received Ctrl-C, exiting...")

    pl.from_dicts(films).rename(
        {
            "release_year": "发布年份",
            "film_name": "电影名称",
            "publish_company": "发行单位",
            "director": "编剧",
            "registration_place": "备案地",
            "description": "梗概",
        }
    ).write_excel("films.xlsx")


if __name__ == "__main__":
    asyncio.run(main())
