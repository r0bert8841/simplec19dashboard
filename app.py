from flask import Flask, render_template, request
import c19functions
import pandas as pd
import os

cwd = os.getcwd()
filepath = cwd + '/data/coronavirus_final.csv'
df = pd.read_csv(filepath, delimiter=',')

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

# @app.route("/data")
# def data():
#     output_df =c19functions.dailySnapshot(df,'2020-05-19','tested')
#     return render_template('data.html',  tables=[output_df.to_html(classes='data')], titles=output_df.columns.values)

@app.route("/data/<value>", methods=['GET', 'POST'])
def data(value):
    value = request.view_args['value']
    date = request.args.get('date_variable')
    #print(date)
    #output_df =c19functions.dailySnapshot(df,'2020-05-19',value)
    output_df =c19functions.dailySnapshot(df,date,value)
    return render_template('data.html',  tables=[output_df.to_html(classes='data')], titles=output_df.columns.values)


@app.route("/about")
def about():
    return render_template("about.html")
       
if __name__ == "__main__":
    app.run(debug=True)