# YoutubeDataHarwesting_warehousing
The aim of the project is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
1. Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, 
   video ID, likes, dislikes, and comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a data lake.
3. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
4. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details

## How to get the project source code??

As this is a public repository, anyone can get the code easily by using the commands **git clone** and **git pull** 

 ## Contents of the Repo :

 There are 2 python files available in the reository.
 1. **YT_Analysis.py** :
     This file has all the front end application code and function calls.
 2. **function_list.py** :
     This file has all functions definitions involved in the project.

## How to run the project??
There is a file named **requirements.txt** available in the project folder. One can install all the required python packages of the project using the below single command.

                    pip install -r requirements.txt

Once the installation of packages are done, we can run the project from **cmd** using the command below..

                    streamlit run pathToFile\YT_Analysis.py

This will open a local browser with port no 8501. The screenshots of the first rendered application are uploaded to the repository.





                    
                    
