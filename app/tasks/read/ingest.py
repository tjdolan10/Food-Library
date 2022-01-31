from recipe_scrapers import scrape_me
import json
import parse_ingredient
from ingredient_parser.en import parse
import os
from sqlalchemy import insert,inspect,select

#for local testing run
projectDir = r"C:\Users\tjdol\Documents\Food-Library"
os.chdir(projectDir)
#local imports
from app.tasks.db import session
from app import models
from app.models.Tables import Recipes

#for local testing run


engine = session.session_func('engine')
sess = session.session_func('session')
print(engine)


#free tier zestful, 30 ingredients per day limit

scraper = scrape_me('https://www.foodandwine.com/recipes/spicy-ginger-pork-lettuce-leaves')
scraper.instructions()
scraper.image()
scraper.host()
# scraper.links() #list of all a tags on webpage
# scraper.nutrients()  # if available
scraper.title()
scraper.total_time()
scraper.yields()
scraper.ingredients()
#free tier zestful, 30 ingredients per day limit
# print(json.dumps(ingredient.as_dict()))
ingredient = parse_ingredient.parse(scraper.ingredients()[0])
ingredient.as_dict()
################
### INGEST! ####
################
# Start by making new recipe (Need to add check if exists)
stmt = insert(models.Tables.Recipes).values(
    name=scraper.title()
    ,description="Stuff and things"
    ,instructions=scraper.instructions())
with engine.connect() as conn:
    result = conn.execute(stmt)


# Add recipe ingredient
stmt = select(Recipes).where(Recipes.name==scraper.title())
with engine.connect() as conn:
    result = conn.execute(stmt)
test2 = result.fetchall()
recipe_id = test2[0][0]



|#add new ingredient (currently no check for duplicates)
stmt = insert(models.Tables.Ingredients).values(
    name=ingredient.as_dict()['product']
    ,description=ingredient.as_dict()['preparationNotes'])
with engine.connect() as conn:
    result = conn.execute(stmt)

#add new measure (currently no check for dups)
stmt = insert(models.Tables.Measures).values(name=ingredient.as_dict()['unit'])
with engine.connect() as conn:
    result = conn.execute(stmt)




# get table columns
mapper = inspect(models.Tables.Recipes)
mapper.columns.keys()

for x in models.Tables.Ingredients.__table__.columns:
    print(x)




scraper.ingredients()[0]
# free method
# test = (parse(scraper.ingredients()[0]))
# print(test)


#read ingredient into database
