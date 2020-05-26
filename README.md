# simplec19dashboard
I want to build a Dashboard that is connector to an api that will give Covid19 information


Population information comes from 
https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx

Scripts:

apiget.sh arg1 - This is a script that takes arg1=State Abbr and calls the API and retrieves and saves the output as a csv file. 
apigetstatets.py - This takes all of the states and runs the apiget.sh script for each state.
combinestatets.py - This takes all of the csv files for each state and appends them into a single dataset.   
prepdata.py - This performs the data manipulation
refreshdata.sh - This runs daily, and runs apigetstatests.py , combinestatets.py, and prepdata.py
