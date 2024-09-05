import json
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from threading import Timer
import os
import time
import snowflake.connector
from app_config import AppConfig
from databricks import sql
tools_list = []

server_hostname = os.get_env('server_name')
http_path = os.get_env('http_path_cluster') 
access_token = os.get_env('access_token') 


def get_info_on_connected_table():
    conn = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    # TODO: check if one or more not provided, return error so user adds it
    db = AppConfig.db
    schema = AppConfig.schema
    table = AppConfig.table
    query = f'''DESCRIBE {db}.{schema}.{table}
'''
    cur = conn.cursor()
    cur.execute(query)

# Fetch all results and load into a pandas DataFrame
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(result, columns=columns)
    print(json.dumps(df.to_dict(orient='records'), indent=2))
    cur.close()
    conn.close() 
    return df.to_string(index=False) + 'THIS IS ONLY INFORMATION FOR YOU. DO NOT SHARE WITH THE USER' 


get_info_on_connected_table_definition = {
    "type": "function",
    "function": {
        "name": "get_info_on_connected_table",
        "description": "Fetches information on the currently connected table including column names, data types, and column descriptions. Returns this information as a formatted string. THIS INFORMATION IS ONLY FOR YOU, DO NOT SHARE WITH USER",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": []
        },
    },
}

def get_table(select_part, additional_query=""): 
    conn = sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=access_token)
    db = AppConfig.db
    schema = AppConfig.schema
    table = AppConfig.table
    # Surely this cant backfire
    # TODO: check if one or more not provided, return error so user adds it
    base_query = f'SELECT {select_part} FROM {db}.{schema}.{table}'
    query_string = f'{base_query} {additional_query}' if additional_query else base_query
    
    # Execute the query and fetch the data
    cur = conn.cursor()
    cur.execute(query_string)
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(result, columns=columns)
    
    filename = f"temp_dataframe_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    
    # storing = good 
    st.session_state.msgs_streamlit.append({
        "role": "assistant", 
        "content": filename, 
        "type": "dataframe", 
        "timestamp": datetime.now().isoformat()  
    })
    # connection.close() 
    st.write(df)
    cur.close()
    conn.close() 
    return 'Data was shown to the user. Ask him for follow ups now'

get_table_definition = {
    "type": "function",
    "function": {
        "name": "get_table",
        "description": "Execute a Snowflake SQL query and retrieve the data as a Pandas DataFrame. You are already connected to some table, which you can check by using get_info_on_connected_table. However, you might need to modify the query on user request. Remember to always put column names into double quotes as per Snowflake syntax. When performing calculations such as averages, ensure numerical fields are cast to FLOAT. YOU ARE NOT ALLOWED TO SEE THE TABLE, BUT IT WILL BE DISPLAYED TO USER AUTOMATICALLY",
        "parameters": {
            "type": "object",
            "properties": {
                "select_part": {
                    "type": "string",
                    "description": "The part of the SQL query that specifies which columns to select (e.g., '*', 'column1, column2'). Ensure numerical fields used for calculations are cast to FLOAT as needed.",
                },
                "additional_query": {
                    "type": "string",
                    "description": "Additional SQL query conditions (e.g., 'WHERE condition', 'LIMIT 10'). When filtering for text, it is suggested to use 'is like' or similar case insensitive searches",
                    "default": ""
                }
            },
            "required": ["select_part"]
        }
    }
}




def generate_vega_lite_spec(mark_type, 
                            x_field=None, x_type=None, x_aggregate=None, x_filter=None, 
                            y_field=None, y_type=None, y_aggregate=None, y_filter=None, 
                            color=None, size=None, column=None, row=None, theta=None, 
                            global_filter=None):
    
        # Retrieve the last DataFrame file path from session state
    last_df_file = None
    for msg in reversed(st.session_state.msgs_streamlit):
        if msg.get("type") == "dataframe":
            last_df_file = msg["content"]
            break

    if last_df_file is None:
        return "No DataFrame found to display."

    try:
        df_from_parquet = pq.read_table(last_df_file).to_pandas()
    except Exception as e:
        return f"Error loading DataFrame: {e}"
    if df_from_parquet.empty:
        return "Loaded DataFrame is empty after filtering."
        
    valid_mark_types = {'bar', 'line', 'point'}
    valid_types = {'quantitative', 'temporal', 'ordinal', 'nominal'}
    
    if mark_type not in valid_mark_types:
        return "Incorrect Mark Type Specified"

    vega_lite_spec = {
        'mark': {'type': mark_type},
        'encoding': {},
        'transform': []
    }

    # Validate and create encoding dictionary
    def create_encoding_dict(field, type, aggregate, filter):
        if type and type not in valid_types:
            return "Incorrect Type Parameter Specified"
        encoding = {}
        if field:
            encoding['field'] = field
        if type:
            encoding['type'] = type
        if aggregate:
            encoding['aggregate'] = aggregate
        if filter:
            encoding['filter'] = filter
        return encoding if encoding else None

    # Add encoding with filter
    x_encoding = create_encoding_dict(x_field, x_type, x_aggregate, x_filter)
    if isinstance(x_encoding, str):
        return x_encoding

    y_encoding = create_encoding_dict(y_field, y_type, y_aggregate, y_filter)
    if isinstance(y_encoding, str):
        return y_encoding

    vega_lite_spec['encoding']['x'] = x_encoding
    vega_lite_spec['encoding']['y'] = y_encoding

    # Handle global filter
    if global_filter:
        vega_lite_spec['transform'].append({'filter': global_filter})

    # Other encodings
    def add_encoding(key, encoding):
        if encoding is not None:
            if isinstance(encoding, dict):
                vega_lite_spec['encoding'][key] = encoding
            else:
                return encoding

    error = add_encoding('color', color) or add_encoding('size', size) or \
            add_encoding('column', column) or add_encoding('row', row) or \
            add_encoding('theta', theta)
    if error:
        return error
    else:
        try:
            st.vega_lite_chart(df_from_parquet, spec=vega_lite_spec, use_container_width=True)
            st.session_state.msgs_streamlit.append({
                "role": "assistant",
                "content": vega_lite_spec,
                "type": "chart",
                "dataframe_file": last_df_file
            })
            return "Chart displayed successfully and specification stored." 
        except Exception as e:
            return f"Error displaying chart: {e}"
            

display_vega_lite_chart_definition =  {
    "type": "function",
    "function": {
        "name": "generate_vega_lite_spec",
        "description": "Generates a Vega-Lite visualization specification. This function allows creating various types of charts (e.g., bar, line, point) and supports detailed encoding configurations for axes and other visual properties. It also includes options for adding filters both globally and per encoding to refine data visibility based on specific conditions.",
        "parameters": {
            "type": "object",
            "properties": {
                "mark_type": {
                    "type": "string",
                    "enum": ["bar", "line", "point"],
                    "description": "Specifies the type of chart to generate. Valid options are 'bar', 'line', 'point'."
                },
                "x_field": {
                    "type": "string",
                    "description": "Field from the data to map against the x-axis."
                },
                "x_type": {
                    "type": "string",
                    "enum": ["quantitative", "temporal", "ordinal", "nominal"],
                    "description": "Type of the x-axis values. Valid types are 'quantitative', 'temporal', 'ordinal', 'nominal'."
                },
                "x_aggregate": {
                    "type": "string",
                    "description": "Aggregation function to apply on the x-axis field, such as 'count', 'mean', etc."
                },
                "x_filter": {
                    "type": "string",
                    "description": "Filter condition to apply to the x-axis values. Remember to use Datum where needed"
                },
                "y_field": {
                    "type": "string",
                    "description": "Field from the data to map against the y-axis."
                },
                "y_type": {
                    "type": "string",
                    "description": "Type of the y-axis values."
                },
                "y_aggregate": {
                    "type": "string",
                    "description": "Aggregation function to apply on the y-axis field."
                },
                "y_filter": {
                    "type": "string",
                    "description": "Filter condition to apply to the y-axis values. Remember to use Datum where needed"
                },
                "global_filter": {
                    "type": "string",
                    "description": "A global filter condition applied to all data in the visualization. Remember to use Datum where needed"
                },
                "color": { 
                    "type": "object", 
                    "description": "Encoding settings for color."
                },
                "size": {
                    "type": "object",
                    "description": "Encoding settings for size."
                },
                "column": {
                    "type": "object",
                    "description": "Specifies how to divide data into vertical facets."
                },
                "row": {
                    "type": "object",
                    "description": "Specifies how to divide data into horizontal facets."
                },
                "theta": {
                    "type": "object",
                    "description": "Encoding settings for angular coordinates in polar charts."
                }
            },
            "required": ["mark_type"]
        }
    }
}

def get_current_weather(location, unit="fahrenheit"):
    """Get the current weather in a given location"""
    if "tokyo" in location.lower():
        return json.dumps({"location": "Tokyo", "temperature": "10", "unit": "celsius"})
    elif "san francisco" in location.lower():
        return json.dumps({"location": "San Francisco", "temperature": "72", "unit": "fahrenheit"})
    elif "paris" in location.lower():
        return json.dumps({"location": "Paris", "temperature": "22", "unit": "celsius"})
    else:
        return json.dumps({"location": location, "temperature": "unknown"})
    

get_current_weather_def = {
                "type": "function",
                "function": {
                    "name": "get_current_weather",
                    "description": "Get the current weather in a given location",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {
                                "type": "string",
                                "description": "The city and state, e.g. San Francisco, CA",
                            },
                            "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]},
                        },
                        "required": ["location"],
                    },
                },
            }

tools_list.append(get_current_weather_def)

tools_list.append(display_vega_lite_chart_definition)
tools_list.append(get_info_on_connected_table_definition)
tools_list.append(get_table_definition)