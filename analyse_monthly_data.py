import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import sqlite3
import os
from datetime import datetime

# Database path
DB_PATH = 'data/energy_data.db'

# Check if database exists
if not os.path.exists(DB_PATH):
    print(f"WARNING: Database not found at {DB_PATH}")
    print("Please run 'python energy_fetcher.py' to fetch data first.")
else:
    print(f"Database found at {DB_PATH}")

# Initialize the Dash app
app = dash.Dash(__name__)
app.title = "Energy Data Dashboard"

# Helper functions
def get_available_months():
    """Get list of available year-months from database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT DISTINCT year, month 
            FROM metering_data 
            ORDER BY year DESC, month DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"DEBUG: Found {len(df)} months in database")
        
        if df.empty:
            print("WARNING: No data found in database. Please run energy_fetcher.py first.")
            return []
        
        months = []
        for _, row in df.iterrows():
            label = f"{row['year']}-{row['month']:02d}"
            value = f"{row['year']}-{row['month']}"
            months.append({'label': label, 'value': value})
        
        return months
    except Exception as e:
        print(f"ERROR loading months: {e}")
        return []

def load_metering_data(year=None, month=None):
    """Load metering data from database."""
    conn = sqlite3.connect(DB_PATH)
    
    if year and month:
        query = """
            SELECT * FROM metering_data 
            WHERE year = ? AND month = ?
        """
        df = pd.read_sql_query(query, conn, params=(year, month))
    else:
        query = "SELECT * FROM metering_data"
        df = pd.read_sql_query(query, conn)
    
    conn.close()
    return df

def load_summary_data(year=None, month=None):
    """Load summary data from database."""
    conn = sqlite3.connect(DB_PATH)
    
    if year and month:
        query = """
            SELECT * FROM monthly_summaries 
            WHERE year = ? AND month = ?
        """
        df = pd.read_sql_query(query, conn, params=(year, month))
    else:
        query = "SELECT * FROM monthly_summaries ORDER BY year, month"
        df = pd.read_sql_query(query, conn)
    
    conn.close()
    return df

def calculate_ratios(year, month):
    """Calculate energy ratios for a specific month."""
    df = load_metering_data(year, month)
    
    if df.empty:
        return {}
    
    print(f"DEBUG: Loaded {len(df)} rows for {year}-{month}")
    print(f"DEBUG: Categories: {df['obis_category'].unique()}")
    print(f"DEBUG: Sample data:\n{df[['obis_code', 'obis_category', 'value']].head()}")
    
    # Map abbreviated categories to full names if needed
    category_map = {'C': 'Consumption', 'P': 'Production'}
    if 'C' in df['obis_category'].values or 'P' in df['obis_category'].values:
        df['obis_category'] = df['obis_category'].map(lambda x: category_map.get(x, x))
    
    # Total consumption (all consumption OBIS codes)
    consumption_df = df[df['obis_category'] == 'Consumption']
    total_consumption = consumption_df['value'].sum()
    print(f"DEBUG: Total consumption from {len(consumption_df)} rows: {total_consumption}")
    
    # Total production (all production OBIS codes)
    production_df = df[df['obis_category'] == 'Production']
    total_production = production_df['value'].sum()
    print(f"DEBUG: Total production from {len(production_df)} rows: {total_production}")
    
    # Measured active consumption (1-1:1.29.0)
    measured_consumption = df[df['obis_code'] == '1-1:1.29.0']['value'].sum()
    
    # Remaining consumption after sharing (1-65:1.29.9) - energy bought from supplier
    energy_bought = df[df['obis_code'] == '1-65:1.29.9']['value'].sum()
    
    # Shared consumption (sum of layers 1-4)
    shared_codes = ['1-65:1.29.1', '1-65:1.29.2', '1-65:1.29.3', '1-65:1.29.4']
    energy_shared_consumption = df[df['obis_code'].isin(shared_codes)]['value'].sum()
    
    # Production metrics
    measured_production = df[df['obis_code'] == '1-1:2.29.0']['value'].sum()
    
    # Shared production (sum of layers 1-4)
    shared_prod_codes = ['1-65:2.29.1', '1-65:2.29.2', '1-65:2.29.3', '1-65:2.29.4']
    energy_shared_production = df[df['obis_code'].isin(shared_prod_codes)]['value'].sum()
    
    # Remaining production after sharing (1-65:2.29.9) - energy sold to market
    energy_sold = df[df['obis_code'] == '1-65:2.29.9']['value'].sum()
    
    # Calculate ratios
    ratios = {
        'total_consumption': round(total_consumption, 2),
        'total_production': round(total_production, 2),
        'measured_consumption': round(measured_consumption, 2),
        'measured_production': round(measured_production, 2),
        'energy_bought': round(energy_bought, 2),
        'energy_shared_consumption': round(energy_shared_consumption, 2),
        'energy_shared_production': round(energy_shared_production, 2),
        'energy_sold': round(energy_sold, 2),
        'production_to_consumption_ratio': round(total_production / total_consumption * 100, 2) if total_consumption > 0 else 0,
        'self_consumption_ratio': round(energy_shared_consumption / measured_consumption * 100, 2) if measured_consumption > 0 else 0,
        'self_sufficiency_ratio': round((measured_consumption - energy_bought) / measured_consumption * 100, 2) if measured_consumption > 0 else 0,
        'energy_bought_ratio': round(energy_bought / measured_consumption * 100, 2) if measured_consumption > 0 else 0,
        'energy_sold_ratio': round(energy_sold / measured_production * 100, 2) if measured_production > 0 else 0,
    }
    
    return ratios

# App layout
app.layout = html.Div([
    html.Div([
        html.H1("Energy Data Dashboard", style={'textAlign': 'center', 'color': '#2c3e50'}),
        html.Hr(),
    ], style={'marginBottom': 30}),
    
    # Month selector
    html.Div([
        html.Label("Select Month:", style={'fontWeight': 'bold', 'fontSize': 16}),
        dcc.Dropdown(
            id='month-selector',
            options=get_available_months(),
            value=get_available_months()[0]['value'] if get_available_months() else None,
            clearable=False,
            style={'width': '300px'}
        ),
    ], style={'marginBottom': 30, 'marginLeft': 20}),
    
    # Key metrics cards
    html.Div(id='metrics-cards', style={'marginBottom': 30}),
    
    # Tabs for different views
    dcc.Tabs([
        # Tab 1: Ratios & Analysis
        dcc.Tab(label='Ratios & Analysis', children=[
            html.Div([
                html.H3("Energy Flow Analysis", style={'marginTop': 20}),
                html.Div(id='ratio-charts'),
            ])
        ]),
        
        # Tab 2: Consumption vs Production
        dcc.Tab(label='Consumption vs Production', children=[
            html.Div([
                html.H3("Monthly Comparison", style={'marginTop': 20}),
                dcc.Graph(id='consumption-production-chart'),
            ])
        ]),
        
        # Tab 3: Trends Over Time
        dcc.Tab(label='Trends Over Time', children=[
            html.Div([
                html.H3("Historical Trends", style={'marginTop': 20}),
                dcc.Graph(id='trends-chart'),
            ])
        ]),
        
        # Tab 4: Detailed Data
        dcc.Tab(label='Detailed Data', children=[
            html.Div([
                html.H3("Metering Data", style={'marginTop': 20}),
                html.Div(id='data-table'),
            ])
        ]),
        
        # Tab 5: Summary by OBIS Code
        dcc.Tab(label='Summary by OBIS', children=[
            html.Div([
                html.H3("Monthly Summary", style={'marginTop': 20}),
                html.Div(id='summary-table'),
            ])
        ]),
    ])
], style={'margin': '20px', 'fontFamily': 'Arial, sans-serif'})

# Callbacks
@app.callback(
    Output('metrics-cards', 'children'),
    Input('month-selector', 'value')
)
def update_metrics(selected_month):
    if not selected_month:
        return html.Div("No data available")
    
    year, month = map(int, selected_month.split('-'))
    ratios = calculate_ratios(year, month)
    
    if not ratios:
        return html.Div("No data available for selected month")
    
    cards = html.Div([
        html.Div([
            html.Div([
                html.H4("Total Consumption", style={'color': '#e74c3c'}),
                html.H2(f"{ratios['total_consumption']} kWh"),
            ], className='metric-card', style={
                'backgroundColor': '#ffebee', 'padding': '20px', 'borderRadius': '10px',
                'textAlign': 'center', 'width': '18%', 'display': 'inline-block', 'margin': '1%'
            }),
            
            html.Div([
                html.H4("Total Production", style={'color': '#27ae60'}),
                html.H2(f"{ratios['total_production']} kWh"),
            ], className='metric-card', style={
                'backgroundColor': '#e8f5e9', 'padding': '20px', 'borderRadius': '10px',
                'textAlign': 'center', 'width': '18%', 'display': 'inline-block', 'margin': '1%'
            }),
            
            html.Div([
                html.H4("Energy Bought", style={'color': '#e67e22'}),
                html.H2(f"{ratios['energy_bought']} kWh"),
                html.P(f"{ratios['energy_bought_ratio']}% of consumption")
            ], className='metric-card', style={
                'backgroundColor': '#fff3e0', 'padding': '20px', 'borderRadius': '10px',
                'textAlign': 'center', 'width': '18%', 'display': 'inline-block', 'margin': '1%'
            }),
            
            html.Div([
                html.H4("Energy Sold", style={'color': '#f39c12'}),
                html.H2(f"{ratios['energy_sold']} kWh"),
                html.P(f"{ratios['energy_sold_ratio']}% of production")
            ], className='metric-card', style={
                'backgroundColor': '#fef5e7', 'padding': '20px', 'borderRadius': '10px',
                'textAlign': 'center', 'width': '18%', 'display': 'inline-block', 'margin': '1%'
            }),
            
            html.Div([
                html.H4("Self-Sufficiency", style={'color': '#3498db'}),
                html.H2(f"{ratios['self_sufficiency_ratio']}%"),
                html.P("Energy covered by sharing")
            ], className='metric-card', style={
                'backgroundColor': '#e3f2fd', 'padding': '20px', 'borderRadius': '10px',
                'textAlign': 'center', 'width': '18%', 'display': 'inline-block', 'margin': '1%'
            }),
        ])
    ])
    
    return cards

@app.callback(
    Output('ratio-charts', 'children'),
    Input('month-selector', 'value')
)
def update_ratio_charts(selected_month):
    if not selected_month:
        return html.Div("No data available")
    
    year, month = map(int, selected_month.split('-'))
    ratios = calculate_ratios(year, month)
    
    if not ratios:
        return html.Div("No data available")
    
    # Consumption breakdown pie chart
    consumption_fig = go.Figure(data=[go.Pie(
        labels=['Energy Shared (Self-Consumption)', 'Energy Bought from Grid'],
        values=[ratios['energy_shared_consumption'], ratios['energy_bought']],
        marker_colors=['#27ae60', '#e74c3c'],
        hole=0.4
    )])
    consumption_fig.update_layout(
        title="Consumption Breakdown",
        height=400
    )
    
    # Production breakdown pie chart
    production_fig = go.Figure(data=[go.Pie(
        labels=['Energy Shared', 'Energy Sold to Market'],
        values=[ratios['energy_shared_production'], ratios['energy_sold']],
        marker_colors=['#3498db', '#f39c12'],
        hole=0.4
    )])
    production_fig.update_layout(
        title="Production Breakdown",
        height=400
    )
    
    # Key ratios bar chart
    ratios_fig = go.Figure(data=[
        go.Bar(
            x=['Production/Consumption', 'Self-Sufficiency', 'Energy Bought', 'Energy Sold'],
            y=[
                ratios['production_to_consumption_ratio'],
                ratios['self_sufficiency_ratio'],
                ratios['energy_bought_ratio'],
                ratios['energy_sold_ratio']
            ],
            marker_color=['#27ae60', '#3498db', '#e74c3c', '#f39c12'],
            text=[f"{v}%" for v in [
                ratios['production_to_consumption_ratio'],
                ratios['self_sufficiency_ratio'],
                ratios['energy_bought_ratio'],
                ratios['energy_sold_ratio']
            ]],
            textposition='auto',
        )
    ])
    ratios_fig.update_layout(
        title="Key Ratios (%)",
        yaxis_title="Percentage",
        height=400
    )
    
    return html.Div([
        html.Div([
            dcc.Graph(figure=consumption_fig, style={'width': '48%', 'display': 'inline-block'}),
            dcc.Graph(figure=production_fig, style={'width': '48%', 'display': 'inline-block', 'marginLeft': '4%'}),
        ]),
        html.Div([
            dcc.Graph(figure=ratios_fig),
        ])
    ])

@app.callback(
    Output('consumption-production-chart', 'figure'),
    Input('month-selector', 'value')
)
def update_comparison_chart(selected_month):
    if not selected_month:
        return go.Figure()
    
    year, month = map(int, selected_month.split('-'))
    df = load_metering_data(year, month)
    
    if df.empty:
        return go.Figure()
    
    # Map abbreviated categories if needed
    category_map = {'C': 'Consumption', 'P': 'Production'}
    if 'C' in df['obis_category'].values or 'P' in df['obis_category'].values:
        df['obis_category'] = df['obis_category'].map(lambda x: category_map.get(x, x))
    
    # Group by entity and category
    grouped = df.groupby(['entity_name', 'obis_category'])['value'].sum().reset_index()
    
    fig = px.bar(
        grouped,
        x='entity_name',
        y='value',
        color='obis_category',
        barmode='group',
        title=f"Consumption vs Production by Entity ({year}-{month:02d})",
        labels={'value': 'Energy (kWh)', 'entity_name': 'Entity'},
        color_discrete_map={'Consumption': '#e74c3c', 'Production': '#27ae60'}
    )
    
    fig.update_layout(height=500)
    
    return fig

@app.callback(
    Output('trends-chart', 'figure'),
    Input('month-selector', 'value')
)
def update_trends_chart(selected_month):
    df = load_summary_data()
    
    if df.empty:
        return go.Figure()
    
    # Map abbreviated categories if needed
    category_map = {'C': 'Consumption', 'P': 'Production'}
    if 'C' in df['obis_category'].values or 'P' in df['obis_category'].values:
        df['obis_category'] = df['obis_category'].map(lambda x: category_map.get(x, x))
    
    # Create year-month label
    df['period'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
    
    # Filter for key OBIS codes
    key_codes = ['1-1:1.29.0', '1-1:2.29.0', '1-65:1.29.9', '1-65:2.29.9']
    df_filtered = df[df['obis_code'].isin(key_codes)]
    
    fig = px.line(
        df_filtered,
        x='period',
        y='total_value',
        color='obis_description',
        markers=True,
        title="Energy Trends Over Time",
        labels={'total_value': 'Energy (kWh)', 'period': 'Month'}
    )
    
    fig.update_layout(height=500, hovermode='x unified')
    
    return fig

@app.callback(
    Output('data-table', 'children'),
    Input('month-selector', 'value')
)
def update_data_table(selected_month):
    if not selected_month:
        return html.Div("No data available")
    
    year, month = map(int, selected_month.split('-'))
    df = load_metering_data(year, month)
    
    if df.empty:
        return html.Div("No data available")
    
    display_cols = ['entity_type', 'entity_name', 'meter_id', 'obis_code', 
                   'obis_category', 'obis_description', 'value', 'unit']
    
    return dash_table.DataTable(
        data=df[display_cols].to_dict('records'),
        columns=[{'name': col, 'id': col} for col in display_cols],
        style_table={'overflowX': 'auto'},
        style_cell={
            'textAlign': 'left',
            'padding': '10px',
            'fontSize': 12
        },
        style_header={
            'backgroundColor': '#2c3e50',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f9f9f9'
            }
        ],
        page_size=20,
        sort_action='native',
        filter_action='native'
    )

@app.callback(
    Output('summary-table', 'children'),
    Input('month-selector', 'value')
)
def update_summary_table(selected_month):
    if not selected_month:
        return html.Div("No data available")
    
    year, month = map(int, selected_month.split('-'))
    df = load_summary_data(year, month)
    
    if df.empty:
        return html.Div("No data available")
    
    display_cols = ['obis_code', 'obis_category', 'obis_description', 
                   'total_value', 'num_meters', 'unit']
    
    return dash_table.DataTable(
        data=df[display_cols].to_dict('records'),
        columns=[{'name': col, 'id': col} for col in display_cols],
        style_table={'overflowX': 'auto'},
        style_cell={
            'textAlign': 'left',
            'padding': '10px',
            'fontSize': 12
        },
        style_header={
            'backgroundColor': '#2c3e50',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f9f9f9'
            },
            {
                'if': {'column_id': 'total_value'},
                'fontWeight': 'bold'
            }
        ],
        sort_action='native'
    )

if __name__ == '__main__':
    print("\n" + "="*80)
    print("Starting Energy Data Dashboard")
    print("="*80)
    print("Access the dashboard at: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop the server")
    print("="*80 + "\n")
    
    app.run(debug=True, host='127.0.0.1', port=8050)