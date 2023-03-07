import asyncio
import pyppeteer
from scraping_scripts import SCRAPE_CHARACTER_PROFILES, SCRAPE_MBTI_SCORES_SCRIPT
from typing import List
from collections import namedtuple
import pandas as pd
import itertools
import pdb

# A named tuple to store the tv show id and the home page of the tv show on `personality_db.com`
tv_show_id_and_page_url = namedtuple(
    "tv_show_id_and_page_url",
    ["tv_show_id", "page_url"],
)


def check_if_votes_above_500(character_profile: List[str], threshold=500):
    """
    return True if the character profile has >500 votes.
    Note: This vote count is sum all personality type votes, not just MBTI. 
    """
    votes = character_profile[2].split('/')[0]
    if 'k' in votes:
        return True
    if int(votes)>500:
        return True
    return False

def clean_up_scraped_MBTI(character_MBTI):
    """
    Clean and reformat MBTI data.
    We keep "ISFJ" scores. eg. An 'E' score would subtracted from 100.
    """
    I_score = int(character_MBTI[1].split('\n')[0][:-1]) if character_MBTI[1].split('\n')[1] == 'I' else (100 - int(character_MBTI[1].split('\n')[0][:-1])) 
    S_score = int(character_MBTI[2].split('\n')[0][:-1]) if character_MBTI[2].split('\n')[1] == 'S' else (100 - int(character_MBTI[2].split('\n')[0][:-1])) 
    F_score = int(character_MBTI[3].split('\n')[0][:-1]) if character_MBTI[3].split('\n')[1] == 'F' else (100 - int(character_MBTI[3].split('\n')[0][:-1])) 
    J_score = int(character_MBTI[4].split('\n')[0][:-1]) if character_MBTI[4].split('\n')[1] == 'J' else (100 - int(character_MBTI[4].split('\n')[0][:-1])) 
    return (character_MBTI[0], I_score , S_score, F_score,  J_score, character_MBTI[5].split()[0])

async def scrape_mbti_from_page(character_url: str):
    """
    Scrape MBTI from single character page
    """
    browser = await pyppeteer.launch({"args": ["--no-sandbox"], "autoClose": False})
    page = await browser.newPage()
    await page.goto(character_url)
    await page.waitForSelector(selector="div.vote-detail-personality", options={'visible':'true'})
    mbti_scores = await page.evaluate(SCRAPE_MBTI_SCORES_SCRIPT)
    await browser.close()
    return mbti_scores

async def get_all_character_profiles_from_tv_show(tv_show_url):
    """
    Fetch links for all popular character pages from the TV show webpage.
    """
    opt = {'headless': True, 'args': ['--no-sandbox'], "autoClose": False}
    browser = await pyppeteer.launch(opt)
    page = await browser.newPage()
    await page.goto(tv_show_url)
    await page.waitForSelector(selector="div.profile-card", options={'visible':'true'})
    all_characters = await page.evaluate(SCRAPE_CHARACTER_PROFILES)
    all_characters = [profile for profile in all_characters if check_if_votes_above_500(profile)]
    character_webpage_urls = [profile[1] for profile in all_characters]
    await browser.close()
    
    tasks = []

    for url in character_webpage_urls:
        tasks.append(
            asyncio.create_task(scrape_mbti_from_page(url))
        )

    scraped_content = await asyncio.gather(*tasks)
    # pdb.set_trace()
    final_mbti_data = []
    for data in zip(all_characters, scraped_content):
        final_mbti_data.append((data[0][0], *data[1]))
    
    return list(map(clean_up_scraped_MBTI, final_mbti_data))

async def scrape_list_of_tv_shows(tv_show_ids_and_homepages: List):
    """
    Scrape and return MBTI from given list of TV show home pages on personality-db . com
    """
    tasks = []

    for url in [show.page_url for show in tv_show_ids_and_homepages]:
        tasks.append(
            asyncio.create_task(get_all_character_profiles_from_tv_show(url))
        )

    scraped_mbti_all_shows = await asyncio.gather(*tasks)
    df_all_shows_mbti = pd.DataFrame(list(itertools.chain(*scraped_mbti_all_shows )), columns=['Name', 'I', 'S', 'F', 'J', 'Votes'])
    df_all_shows_mbti['Tv_Show_ID'] = pd.Series([show.tv_show_id for i,show in enumerate(tv_show_ids_and_homepages) for _ in range(len(scraped_mbti_all_shows [i]))])
    df_all_shows_mbti['Character_ID'] = pd.Series([f"{show.tv_show_id}_{j}" for i,show in enumerate(tv_show_ids_and_homepages) for j in range(len(scraped_mbti_all_shows [i]))])
    return df_all_shows_mbti



if __name__ == "__main__":
    tv_show_ids_and_homepages = [
        tv_show_id_and_page_url('92dc8d27', 'https://www.personality-database.com/profile?pid=2&cid=2&sub_cat_id=217'),
    tv_show_id_and_page_url('5e4dea8f', 'https://www.personality-database.com/profile?pid=2&cid=2&sub_cat_id=2'),
    tv_show_id_and_page_url('c0853535', 'https://www.personality-database.com/profile?pid=2&cid=2&sub_cat_id=3'),
    tv_show_id_and_page_url('280b20ea', 'https://www.personality-database.com/profile?pid=2&cid=2&sub_cat_id=317'),
    ]
    results = asyncio.run(
        scrape_list_of_tv_shows(tv_show_ids_and_homepages)
        )
    results.to_csv("mbti_scraped_1.csv", index=False)