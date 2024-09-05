from tools_list import *  
import litellm
import json
import streamlit as st
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from litellm import completion, completion_cost, model_cost
from app_config import AppConfig

api_key = os.getenv('llm_api_key')
model_to_use = os.getenv('llm_model_to_use')

st.title('Simple Chatbot with Tools')
with st.sidebar:
    db = st.sidebar.text_input("Database", "")
    schema = st.sidebar.text_input("Schema", "")
    table = st.sidebar.text_input("Table", "")
# Set the configuration values
AppConfig.set_config(db, schema, table)
litellm.set_verbose=True
if "conversation"  not in st.session_state:
    st.session_state.conversation = []

CHOSEN_MODEL=model_to_use
CONVERSATION_LIST = st.session_state.conversation
CHOSEN_TOOLS_LIST = tools_list
llm_api_version = os.getenv('llm_api_version')
llm_api_base = os.getenv('llm_api_base')

def add_user_msg(user_msg):
    st.session_state.conversation.append({"role": "user", "content": user_msg})

def add_assistant_msg(assistant_msg):
    st.session_state.conversation.append(assistant_msg)


def add_tool_msg(tool_call,function_name,function_response):
    st.session_state.conversation.append(
          {
              "tool_call_id": tool_call.id,
              "role": "tool",
              "name": function_name,
              "content": function_response,
          }
      )

def process_tool_use(tool_calls):

    for tool_call in tool_calls:
        function_name = tool_call.function.name
        function_to_call = globals().get(function_name)

        if function_to_call is None:
            function_response = "Wrong function name"
            add_tool_msg(tool_call, function_name, function_response)
            return function_response
        
        try:
            function_args = json.loads(tool_call.function.arguments)
        except json.JSONDecodeError as e:
            function_response = f"Invalid function arguments: {e}"
            add_tool_msg(tool_call, function_name, function_response)
            return function_response
        
        try:
            function_response = function_to_call(**function_args)
        except TypeError as e:
            function_response = f"Wrong parameters provided: {e}"
            add_tool_msg(tool_call, function_name, function_response)
            return function_response
        except Exception as e:
            function_response = f"An error occurred in the function: {e}"
            add_tool_msg(tool_call, function_name, function_response)
            return function_response

        add_tool_msg(tool_call, function_name, function_response)
        
    return function_response


def call_llm(chosen_model=CHOSEN_MODEL, conversation_list=CONVERSATION_LIST, chosen_tools_list=CHOSEN_TOOLS_LIST):
    
    # litellm.set_verbose=True # debug
    try:
        if chosen_tools_list:
            response = litellm.completion(
                model=chosen_model,
                messages=conversation_list,
                tools=chosen_tools_list,
                tool_choice="auto",
                
                api_key=api_key,
                api_base = llm_api_base,
                api_version = llm_api_version
                )
        else:
            response = litellm.completion(
                model=chosen_model,
                messages=conversation_list,
                api_key = api_key
            )
        add_assistant_msg(response.choices[0].message)
        print("{:.8f}".format(response._hidden_params["response_cost"]))
        print(conversation_list)        
    except Exception as e:
        print("An error occurred:", e)
    print(response)
    return response

if "msgs_streamlit" not in st.session_state:
    st.session_state["msgs_streamlit"] = []

for msg in st.session_state.msgs_streamlit:
    if msg.get("type") == "dataframe":
        df_from_parquet = pq.read_table(msg["content"]).to_pandas()
        st.write(df_from_parquet)
    elif msg.get("type") == "chart":
        st.vega_lite_chart(df_from_parquet, spec=msg["content"], use_container_width=True)
    else:
        if msg["content"] is not None:# and msg["role"] in ['user','assistant']:
            st.chat_message(msg["role"]).write(msg["content"])


def stream_data(all_msg): 
    for word in all_msg.split(" "):
        yield word + " "
        time.sleep(0.02)

def process_response(response):
    response_message = response.choices[0].message
    st.session_state.msgs_streamlit.append({"role": "assistant", "content": response_message.content})
    if response_message.content:
        st.chat_message("assistant").write_stream(stream_data(response_message.content))         
    return getattr(response_message, 'tool_calls', None)

system_message = '''
You are Helpful Assistant
'''
if user_input := st.chat_input():
    CONVERSATION_LIST.append({"role": "system", "content": system_message})
    add_user_msg(user_input)
    st.session_state.msgs_streamlit.append({"role": "user", "content": user_input})
    st.chat_message("user").write(user_input)
    response = call_llm()
    tool_calls = process_response(response)
    while tool_calls:
        process_tool_use(tool_calls)
        response = call_llm()
        tool_calls = process_response(response)





