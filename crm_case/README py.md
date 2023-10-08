```python
#1. Setting Environment (install all dependency like node, npm, building pipenv directory via terminal) and store dataset into same environment  
#2. Import Library 
import dash                     # pip install dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go 
import plotly.express as px     # pip install plotly==5.2.2
import pandas as pd             # pip install pandas


# 3. Reading Dataset into DataFrame 
df= pd.read_csv('LuxuryLoanPortfolio.csv',delimiter=',') #reading dataset
df['funded_date'] = pd.to_datetime(df['funded_date']) #change funded date into date_time 
df['total_repayment']=df['payments']*df['total past payments'] #calculate total payment 
df['Year'] = df['funded_date'].apply(lambda x:x.year) #strip year from funded_date 

#4. Using external stylesheet 
external_stylesheets= ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#5. Building HTML Component to control Dash Layout 
app.layout = html.Div([
    html.H1(id = 'H1', children = 'Analytics Dashboard of Luxury Loan Portfolio 2012 -2019', style = {'textAlign':'center',\
                                            'marginTop':40,'marginBottom':40}),
    html.Hr(),
    html.P("Choose year:"),
    html.Div(html.Div([
        dcc.Dropdown(id='Year', clearable=False,   #Create Dropdown based on Year
                     value="2019",
                     options=[{'label': x, 'value': x} for x in
                             sorted(df["Year"].unique())]),
    ],className="two columns"),className="row"),

    html.Div(id="output-div", children=[]),
])

@app.callback(Output(component_id="output-div", component_property="children"),
              Input(component_id="Year", component_property="value")
)

#6. Building Graph Function 
def make_graphs(year_chosen): 
    # Histogram of funded amount per term length 
    df_hist =df[df['Year']==year_chosen]
    fig_hist = px.histogram(df_hist,x ='duration months', nbins = 10, 
                           title = 'Distribution of Funded Amount per Duration Length')
    fig_hist.update_xaxes(categoryorder = 'total descending')
    fig_hist.update_layout(bargap=0.1)
    
    # Strip Chart of funded amount per purpose
    fig_strip = px.strip(df_hist, x ='funded_amount',y='purpose', color = 'purpose', title = 'Funded Amount by Purpose')
    
    # BAR Chart of total repayment per job title 
    df_bar = df_hist.sort_values(by='loan balance', ascending = False)
    fig_bar = px.bar(df_bar, x='loan balance', y='BUILDING CLASS CATEGORY', title = 'Loan Balance per Building Class Category')



    return[
        html.Div([
            html.Div([dcc.Graph(figure=fig_hist)], className="six columns"),
            html.Div([dcc.Graph(figure=fig_strip)], className="six columns"),
            html.Div([dcc.Graph(figure = fig_bar)],className = 'twelve columns'),
        ], className="row"), 
    ]



if __name__ == '__main__':
    app.run_server(debug=False)
```

    <ipython-input-1-0826f24c94ab>:3: UserWarning: 
    The dash_core_components package is deprecated. Please replace
    `import dash_core_components as dcc` with `from dash import dcc`
      import dash_core_components as dcc
    <ipython-input-1-0826f24c94ab>:4: UserWarning: 
    The dash_html_components package is deprecated. Please replace
    `import dash_html_components as html` with `from dash import html`
      import dash_html_components as html


    Dash is running on http://127.0.0.1:8050/
    
     * Serving Flask app "__main__" (lazy loading)
     * Environment: production
    [31m   WARNING: This is a development server. Do not use it in a production deployment.[0m
    [2m   Use a production WSGI server instead.[0m
     * Debug mode: off


     * Running on http://127.0.0.1:8050/ (Press CTRL+C to quit)
    127.0.0.1 - - [08/Dec/2021 19:14:03] "[37mGET / HTTP/1.1[0m" 200 -
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[37mGET /_dash-layout HTTP/1.1[0m" 200 -
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[37mGET /_dash-dependencies HTTP/1.1[0m" 200 -
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[36mGET /_dash-component-suites/dash/dcc/async-dropdown.js HTTP/1.1[0m" 304 -
    /Users/tiatitisari/opt/anaconda3/lib/python3.8/site-packages/pandas/core/ops/array_ops.py:253: FutureWarning:
    
    elementwise comparison failed; returning scalar instead, but in the future will perform elementwise comparison
    
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[37mPOST /_dash-update-component HTTP/1.1[0m" 200 -
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[36mGET /_dash-component-suites/dash/dcc/async-graph.js HTTP/1.1[0m" 304 -
    127.0.0.1 - - [08/Dec/2021 19:14:04] "[36mGET /_dash-component-suites/dash/dcc/async-plotlyjs.js HTTP/1.1[0m" 304 -
    127.0.0.1 - - [08/Dec/2021 19:14:12] "[37mPOST /_dash-update-component HTTP/1.1[0m" 200 -
    127.0.0.1 - - [08/Dec/2021 19:14:20] "[37mPOST /_dash-update-component HTTP/1.1[0m" 200 -

