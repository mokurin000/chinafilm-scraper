import asyncio
from functools import partial
from typing import TypedDict

import polars as pl
from diskcache import Cache
from bs4 import BeautifulSoup, Tag
from aiohttp import ClientSession
from loguru import logger

FILM_CACHE = Cache("temp")


class Film(TypedDict):
    release_year: str
    film_name: str
    publish_company: str
    description: str
    registration_place: str


def extract_sub_page(soup: BeautifulSoup) -> list[str]:
    def atag_to_href(atag: Tag) -> str:
        return atag["href"]

    return list(map(atag_to_href, soup.select("li > a.m2r_a")))


async def fill_description(session: ClientSession, film: Film):
    hash_key = str(film).encode("utf-8")
    cache = FILM_CACHE.get(hash_key)
    if cache is not None:
        return cache

    logger.info(f"fetching description for film {film['film_name']}")

    url = film["description"]
    async with session.get(url) as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")
    description: str = soup.select_one("tr:nth-child(8) > td:nth-child(2)").text
    film["description"] = description.strip()[4:]

    FILM_CACHE.add(hash_key, film)

    return film


async def extract_page(session: ClientSession, url: str) -> list[Film]:
    logger.info(f"start scraping for {url}")

    films: list[Film] = []

    release_year = url.split("/")[1][:4]

    async with session.get(url) as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")

    film_records = soup.select("tr:not(:first-child)")
    for film_record in film_records:
        _detail_link = film_record.select_one("td:nth-child(2) > a")["href"]
        film_name: str = film_record.select_one("td:nth-child(3)").text
        publish_company: str = film_record.select_one(
            "td:nth-child(4) > script"
        ).text.split("'")[1]
        registration_place: str = film_record.select_one("td:last-child").text

        films.append(
            Film(
                description=_detail_link,
                release_year=release_year,
                film_name=film_name.strip(),
                publish_company=publish_company.strip(),
                registration_place=registration_place.strip(),
            )
        )

    return [await fill_description(session, film) for film in films]


async def extract_page_url(session: ClientSession, page_num: int) -> list[str]:
    async with session.get(f"index_{page_num}.html") as resp:
        document = await resp.text()

    soup = BeautifulSoup(document, features="lxml")
    return extract_sub_page(soup)


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

    extractor = partial(extract_page_url, session)

    for pages_part in await asyncio.gather(
        *(extractor(page_num) for page_num in range(1, page_count))
    ):
        pages.extend(pages_part)

    return pages


async def main():
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
        except KeyboardInterrupt:
            pass
        except:  # noqa: E722
            pass

    pl.from_dicts(films).select(
        [
            "release_year",
            "film_name",
            "publish_company",
            "registration_place",
            "description",
        ]
    ).rename(
        {
            "release_year": "发布年份",
            "film_name": "电影名称",
            "publish_company": "发行单位",
            "registration_place": "备案地",
            "description": "梗概",
        }
    ).write_excel("films.xlsx")


if __name__ == "__main__":
    asyncio.run(main())
