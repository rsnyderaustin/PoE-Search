
from src.poe_search.wiki_api.pull import WikiImageUrlPull
from src.poe_search.updating.updates import *

# df = update_skills()
# df = update_skills()
# print(df)

ip = WikiImageUrlPull(file_name="File:Absolution_skill_icon.png")
url = ip.fetch_image_url()
print(url)
