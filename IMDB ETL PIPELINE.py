import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys
from pandas.io import gbq

#EXTRACT
#Start the driver
driver = webdriver.Chrome('C:/Users/AYOTOMIWA BAMISAIYE/Desktop/chromedriver_win32/chromedriver.exe')
driver.get('https://www.google.com/') 
driver.maximize_window() 

#Go to the starting webpage which is google.com
search_box = driver.find_element('xpath', '/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input')
#Inputs text into the google search box
search_box.send_keys('imdb top 1000 movies of all time')
#Press enter button to search
search_box.send_keys(Keys.ENTER) 
#Press on the link for IMDB
imdb_page = driver.find_element('xpath', '//*[@id="rso"]/div[3]/div/div/div[1]/div/a/h3').click()

#Extract the HTML into python
soup = BeautifulSoup(driver.page_source, 'lxml') 

#Create a Dataframe with the required columns
df = pd.DataFrame({'RANKING':[''], 'TITLE':[''], 'YEAR':[''], 'DURATION':[''], 'GENRE':[''], 'RATING':[''], 
                   'METASCORE':[''], 'PLOT':[''], 'DIRECTOR':[''], 'MOVIE_STARS':[''], 'VOTE':[''], 'GROSS':['']})

#Get the required data from the HTML and add it to your Dataframe
while True:
    postings = soup.find_all('div', {'class', 'lister-item mode-detail'})
    for post in postings:
        try:
            #RANKING
            ranking = post.find('span', {'class':'lister-item-index unbold text-primary'}).text.strip()[:-1]
            #TITLE
            title = post.find('h3', {'class':'lister-item-header'}).find('a').text.strip()
            #YEAR
            year = post.find('span', {'class':'lister-item-year text-muted unbold'}).text.strip()
            #DURATION
            duration = post.find('span', {'class':'runtime'}).text.strip()
            #GENRE
            genre = post.find('span', {'class':'genre'}).text.strip()
            #RATING
            rating = post.find('span', {'class':'ipl-rating-star__rating'}).text.strip()   
            #METASCORE
            metascore = post.find('span', {'class':'favorable'}).text.strip()
            #PLOT
            plot = post.find('p', {'class':''}).text.strip() 
            #DIRECTOR
            director = post.find_all('p', {'class':'text-muted text-small'})[1].find('a').text.strip()
            #STARS
            stars = post.find_all('p', {'class':'text-muted text-small'})[1].find_all('a')[1:]
            stars2 = []
            for i in stars:
                name = i.text.strip()
                stars2.append(name)
            movie_stars = stars2[0] + ', ' + stars2[1] + ', ' + stars2[2] + ', ' + stars2[3]
            movie_stars
            #VOTE 
            vote = post.find('span', {'name':'nv'}).text.strip()
            #GROSS
            gross = post.find_all('span', {'name':'nv'})[1].text.strip()
        except:
            pass
        df = df.append({'RANKING':ranking, 'TITLE':title, 'YEAR':year, 'DURATION':duration, 'GENRE':genre, 'RATING':rating, 'METASCORE':metascore, 
                        'PLOT':plot, 'DIRECTOR':director, 'MOVIE_STARS':movie_stars, 'VOTE':vote, 'GROSS':gross}, ignore_index = True)
        
    #Loop through all the pages to get the required data from the HTML and add it to your Dataframe
    next_page = soup.find('a', {'class':'flat-button lister-page-next next-page'}).get('href')
    next_page_full = 'https://www.imdb.com' + next_page
    driver.get(next_page_full)
    soup = BeautifulSoup(driver.page_source, 'lxml') 

#TRANSFORM
#MOST OF THE TRANSFORMATION WAS DONE DURING THE EXTRACTION STAGE

#Search for duplicates
if len(df['RANKING'].unique()) < len(df):
    print('There are duplicates in the data') 
#Search for missing titles
if df['TITLE'].isnull().any():
    print('There are missing values in this dataset')

#Exclude the 1st row
df = df[1:]

#LOAD
#Connect to google cloud api and upload dataframe
#Ensure Google Cloud SDK has been installed and authenticated
df.to_gbq(destination_table = 'ETL_PIPELINE.IMDB_TOP_1000', project_id = 'new-project-365804', if_exists = 'fail')  
