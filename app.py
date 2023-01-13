# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

from distutils import core
import seaborn as sns
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as colors
import matplotlib.cm as cmx
from matplotlib.pyplot import figure
import plotly.express as px
import datetime


st.set_page_config(layout="wide")

st.title('Epsilon PCM Job Analysis')
st.subheader("Hosted on Cloudera Machine Learning")

##########
#Datasets
##########
data = pd.read_csv("dataset/All_Jobs_Data.csv")
df = data
final_df5 = pd.read_csv("dataset/five_core_8.csv")
final_df2 = pd.read_csv("dataset/two_core_8.csv")
final_df4 = pd.read_csv("dataset/four_core_8.csv")
final_df14 = pd.read_csv("dataset/six_core_14.csv")
final_df24 = pd.read_csv("dataset/six_core_24.csv")
final_df241 = pd.read_csv("dataset/six_core_24_exec_1.csv")
grafana_df = pd.read_csv("dataset/grafana_data_export.csv")
##########
#Datasets
##########

##########
#Functions
##########


def calculate_error(error_df):
    col1, col2 = st.columns(2)
    error_data_count = error_df.groupby(["status", "Error_Type"]).count()
    error_data_mean = error_df.groupby(["status", "Error_Type"]).mean()
    error_data_org = error_df.groupby(["orgId", "Error_Type"]).count()
    y = error_df["id"].value_counts()
    fig = px.histogram(error_df, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='Error_Type', text_auto=True)
    fig.update_xaxes(type ="category" )
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    fig_org = px.histogram(error_df, x="Error_Type", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
    fig_org.update_xaxes(type ="category" )
    fig_org.update_layout(height=700)
    fig_org.update_layout(width=1100)
    with col1:
      st.write(""" ### Error Count by Type""")
      st.plotly_chart(fig)
      st.dataframe(error_data_count["id"])
    error_line = error_df
    fig = px.line(error_line, x = 'id', y = 'runtime', color = 'Error_Type')
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    with col2:
      st.write(""" ### Average Error Runtime by Type""")
      st.plotly_chart(fig)
      st.dataframe(error_data_mean["runtime"])
    col1, col2 = st.columns([2,1])
    with col1:
      st.plotly_chart(fig_org)
      fig = px.line(error_line, x = 'id', y = 'waitTime', color = 'Error_Type')
      fig.update_layout(height=500)
      fig.update_layout(width=800)
      st.write(""" ### Average Wait Time by Type""")
      st.plotly_chart(fig)
      st.dataframe(error_data_mean["waitTime"])
    with col2:
      st.write(""" ### Error Types By Orgs
      """)
      st.dataframe(error_data_org.id)
    return error_df

def query_data(input_data, status, error, *org):

  return data_source

def job_function(data_source, core, mem):
  with st.expander("Show Data"):
    st.dataframe(data_source)
  
  col1, col2, col3 = st.columns(3)
  
  with col1:
    job_status_function = st.multiselect(f"Select {core} core {mem}GB  Job Status",data_source["status"].unique(), default = data_source["status"].unique() )
  with col2:
    job_error_function = st.multiselect(f"Select {core} core {mem}GB Job Cores",data_source["Error_Type"].unique(), default = data_source["Error_Type"].unique() )
  with col3:
    org_option = st.checkbox(f"Data Per Organisation for {core} cores {mem}GB")
  
  if org_option:
    job_org_function = st.multiselect(f"Select Job Org IDs",data_source["orgId"].unique(), default = data_source["orgId"].unique())
  else:
    job_org_function = data_source["orgId"].unique()
  
  if st.button(f'Query {core} cores {mem} GB'):
    input_data = data_source
    input_data = input_data[input_data["status"].isin(job_status_function) & input_data["Error_Type"].isin(job_error_function)]
    input_data = input_data[input_data["orgId"].isin(job_org_function)]
    st.dataframe(input_data)
    col1, col2, col3 = st.columns(3)
    with col1:
      st.write(f" Dataframe has {len(input_data)} rows")
      st.write(f" Average Job Runtime {input_data.runtime.mean()} minutes")
      st.write(f" Average Job WaitTime {input_data.waitTime.mean()} minutes")
      st.write(f" Successful Jobs {len(input_data[input_data.status == 'succeeded'])} jobs")
    calculate_error(input_data)
  return data_source  

##########
#Functions
##########


########## 
# Query Tool
########## 
st.write(""" ## Query Tool""")

def query_data(status, cores, memory, init_exec, *org ):
    df = data
    df = df[df["status"].isin(job_status_option) & df["cores"].isin(job_cores_option) & df["memory"].isin(job_memory_option) & df["init_exec"].isin(job_init_exec_option)]
    df = df[df["orgId"].isin(job_org_option)]
    st.dataframe(df)
    return df
    
col1, col2, col3, col4 = st.columns(4)

with col1:
  job_status_option = st.multiselect("Select Job Status",data["status"].unique(), default = data["status"].unique() )
with col2:
  job_cores_option = st.multiselect("Select Job Cores",data["cores"].unique(), default = data["cores"].unique() )
with col3:
  job_memory_option = st.multiselect("Select Job Memory",data["memory"].unique(), default = data["memory"].unique() )
with col4:
  job_init_exec_option = st.multiselect("Select Initial Executor",data["init_exec"].unique(), default = data["init_exec"].unique() )
  org_option = st.checkbox("Data Per Organisation")
  

if org_option:
    job_org_option = st.multiselect("Select Job Org IDs",data["orgId"].unique(), default = data["orgId"].unique())
else:
    job_org_option = data["orgId"].unique()

if st.button('Query'):
    show_data = query_data(job_status_option, job_cores_option, job_memory_option, job_init_exec_option ,job_org_option)
    st.write(f" Dataframe has {len(show_data)} rows")
    st.write(f" Average Job Runtime {show_data.runtime.mean()} minutes")
    st.write(f" Max Job Runtime {show_data.runtime.max()} minutes")
    st.write(f" Min Job Runtime {show_data.runtime.min()} minutes")
    st.write(f" Median Job Runtime {show_data.runtime.median()} minutes")
    st.write(f" Successful Jobs {len(show_data[show_data.status == 'succeeded'])} jobs")

########## 
# Query Tool
########## 
    
    
    
#########
#Per Configuration Analysis
#########

st.write(""" ## Job Analysis """)
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(["All Jobs", "5 Cores 8 GB", "2 Cores 8 GB", "4 Cores 8GB", "6 cores 14 GB", "6 cores 24 GB", "Compare(Beta)","6 core 24 GB Exec 1", "Nodes"])

with tab1:
    #########
    # Job Status Per Core
    ########
    st.write("## Job Status Per Core")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
      option = st.selectbox('Cores',('All', 2, 4, 5, 6))
      if option == "All":
        job_distr = df.groupby(["status", "cores"]).count()
        job_distr = job_distr["id"]
        st.dataframe(job_distr)
      else:
        df = df[df["cores"] == option]
        job_distr = df.groupby(["status", "cores"]).count()
        job_distr = job_distr["id"]
        st.dataframe(job_distr)
        #calculations = pd.dataframe()  
    
    with col2:
      if option == "All":
        df = df
        y = df["id"].value_counts()
        fig = px.histogram(df, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='cores', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      else:
        df = df[df["cores"] == option]
        y = df["id"].value_counts()
        fig = px.histogram(df, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='cores', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
    
    
    with col1:
      st.write(f"Total Failed Jobs: **{df[df.status == 'failed'].shape[0]}**")
      st.write(f"Total Succeeded Jobs: **{df[df.status == 'succeeded'].shape[0]}**")
      st.write(f"Total Killed Jobs: **{df[df.status == 'killed'].shape[0]}**")
    
    ##########
    # Job Runtimes
    ##########
    
    st.write("## Job Runtimes")
    
    col1, col2, col3 = st.columns(3)
    
    with col3:
    
      option1 = st.selectbox('Job Status',('All',"succeeded", "failed"))
      option2 = st.selectbox('Job Cores',('All', 2, 4, 5, 6))
      if option1 == "All" and option2 == "All":
        line_data = data
        runtime_mean = line_data.groupby(['status','cores']).mean()
        runtime_mean = runtime_mean["runtime"]
        st.write("Average Job Runtimes")
        st.write(runtime_mean)  
      elif option1 != "All" and option2 == "All":
        line_data = data
        line_data = line_data[line_data["status"] == option1]
        runtime_mean = line_data.groupby(['status','cores']).mean()
        runtime_mean = runtime_mean["runtime"]
        st.write("Average Job Runtimes")
        st.write(runtime_mean)  
      elif option1 == "All" and option2 != "All":
        line_data = data
        line_data = line_data[line_data["cores"] == option2]
        runtime_mean = line_data.groupby(['status','cores']).mean()
        runtime_mean = runtime_mean["runtime"]
        st.write("Average Job Runtimes")
        st.write(runtime_mean)
        if option2 == 6:
          st.write(""" 
          6 vCPU jobs have been run with **14 GB and 24 GB memory**
          """)
      else:
        line_data = data
        line_data = line_data[line_data["status"] == option1]
        line_data = line_data[line_data["cores"] == option2]
        runtime_mean = line_data.groupby(['status','cores']).mean()
        runtime_mean = runtime_mean["runtime"]
        st.write("Average Job Runtimes")
        st.write(runtime_mean)
        if option2 == 6:
          st.write(""" 
          6 vCPU jobs have been run with **14 GB and 24 GB memory**
          """)
        
    with col1:
      if option1 == "All" and option2 == "All":
        line_data = data
        fig = px.line(line_data, x = 'id', y = 'runtime', color = 'cores')
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)  
      elif option1 != "All" and option2 == "All":
        line_data = data
        line_data = line_data[line_data["status"] == option1]
        fig = px.line(line_data, x = 'id', y = 'runtime', color = 'cores')
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      elif option1 == "All" and option2 != "All":
        line_data = data
        line_data = line_data[line_data["cores"] == option2]
        fig = px.line(line_data, x = 'id', y = 'runtime', color = 'status')
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      else:
        line_data = data
        line_data = line_data[line_data["status"] == option1]
        line_data = line_data[line_data["cores"] == option2]
        fig = px.line(line_data, x = 'id', y = 'runtime', color = 'cores')
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
    
    
    st.write("""
    ## Jobs By Orgs
    """)
    col1, col2, col3 = st.columns(3)
    data = data
    
    with col1:
    
      org_option1 = st.selectbox('Job Status By Orgs',('All',"succeeded", "failed"))
      org_option2 = st.selectbox('Job Cores By Orgs',('All', 2, 4, 5, 6))
      per_org = st.checkbox("Data Per Org")
    
      if per_org:
        org_list = st.multiselect("Select Organisations",data["orgId"].unique())
        data = data[data["orgId"].isin(org_list)]
    
      if org_option1 == "All" and org_option2 == "All":
        org_data = data
        org_count = org_data.groupby(['orgId','status']).count()
        org_count = org_count["id"].sort_values(ascending=False)
        st.write("Job Count By Orgs")
        st.write(org_count)  
      elif org_option1 != "All" and org_option2 == "All":
        org_data = data
        org_data = org_data[org_data["status"] == org_option1]
        org_count = org_data.groupby(['orgId','status']).count()
        org_count = org_count["id"].sort_values(ascending=False)
        st.write("Job Count By Orgs")
        st.write(org_count)  
      elif org_option1 == "All" and org_option2 != "All":
        org_data = data
        org_data = org_data[org_data["cores"] == org_option2]
        org_count = org_data.groupby(['orgId','status']).count()
        org_count = org_count["id"].sort_values(ascending=False)
        st.write("Job Count By Orgs")
        st.write(org_count)  
        if org_option2 == 6:
          st.write(""" 
          6 vCPU jobs have been run with **14 GB and 24 GB memory**
          """)
      else:
        org_data = data
        org_data = org_data[org_data["status"] == org_option1]
        org_data = org_data[org_data["cores"] == org_option2]
        org_count = org_data.groupby(['orgId','status']).count()
        org_count = org_count["id"].sort_values(ascending=False)
        st.write("Job Count By Orgs")
        st.write(org_count)
        if org_option2 == 6:
          st.write(""" 
          6 vCPU jobs have been run with **14 GB and 24 GB memory**
          """)
        
    with col2:
      if org_option1 == "All" and org_option2 == "All":
        line_data = data
        y = line_data["id"].value_counts()
        fig = px.histogram(line_data, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      elif org_option1 != "All" and org_option2 == "All":
        line_data = data
        line_data = line_data[line_data["status"] == org_option1]
        y = line_data["id"].value_counts()
        fig = px.histogram(line_data, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      elif org_option1 == "All" and org_option2 != "All":
        line_data = data
        line_data = line_data[line_data["cores"] == org_option2]
        y = line_data["id"].value_counts()
        fig = px.histogram(line_data, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)
      else:
        line_data = data
        line_data = line_data[line_data["status"] == org_option1]
        line_data = line_data[line_data["cores"] == org_option2]
        y = line_data["id"].value_counts()
        fig = px.histogram(line_data, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
        fig.update_xaxes(type ="category" )
        fig.update_layout(height=700)
        fig.update_layout(width=1000)
        st.plotly_chart(fig)

        
with tab2:
  job_function(final_df5, 5, 8)

with tab3:
  job_function(final_df2, 2, 8)

with tab4:
  job_function(final_df4, 4, 8)

with tab5:
  job_function(final_df14, 6, 14)

with tab6:
  job_function(final_df24, 6, 24)

with tab7:
  def compare_conf_table(conf_data, conf_str):
    conf_data_count = conf_data.groupby(["status", "Error_Type"]).count()
    conf_data_mean = conf_data.groupby(["status", "Error_Type"]).mean()
    conf_data_org = conf_data.groupby(["orgId", "Error_Type"]).count()
    st.write(f"""{conf_str} Error Count by Type""")
    st.dataframe(conf_data_count["id"])
    st.write(f"""{conf_str} Average Error Runtime by Type""")
    st.dataframe(conf_data_mean["runtime"])
    st.write(f"""{conf_str} Error Types By Orgs""")
    st.dataframe(conf_data_org.id)
    return conf_data
  st.write(""" Compare two Configurations""")
  def compare_conf(conf_data, conf_str):
    conf_data_count = conf_data.groupby(["status", "Error_Type"]).count()
    conf_data_mean = conf_data.groupby(["status", "Error_Type"]).mean()
    conf_data_org = conf_data.groupby(["orgId", "Error_Type"]).count()
    y = conf_data["id"].value_counts()
    fig = px.histogram(conf_data, x="status", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='Error_Type', text_auto=True)
    fig.update_xaxes(type ="category" )
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    fig_org = px.histogram(conf_data, x="Error_Type", y=y, barmode="group",category_orders=dict(Cores=[2, 4, 5, 6]), color='orgId', text_auto=True)
    fig_org.update_xaxes(type ="category" )
    fig_org.update_layout(height=500)
    fig_org.update_layout(width=800)
    st.write(f""" ### {conf_str} Error Count by Type""")
    st.plotly_chart(fig)
    st.dataframe(conf_data_count["id"])
    conf_line = conf_data
    fig = px.line(conf_line, x = 'id', y = 'runtime', color = 'Error_Type')
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    st.write(f""" ### {conf_str} Average Error Runtime by Type""")
    st.plotly_chart(fig)
    st.dataframe(conf_data_mean["runtime"])
    st.write(f""" ### {conf_str} Error Types By Orgs""")
    st.plotly_chart(fig_org)  
    st.dataframe(conf_data_org.id)
    fig = px.line(conf_line, x = 'id', y = 'waitTime', color = 'Error_Type')
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    st.write(""" ### Average Wait Time by Type""")
    st.plotly_chart(fig)
    st.dataframe(conf_data_mean["waitTime"])
    return conf_data  
#  col1, col2, col3, col4, col5 = st.columns(5)
#  with col1:
#    compare_conf_table(final_df5,'5 core 8GB')
#  with col2:
#    compare_conf_table(final_df2, '2 core 8GB')
#  with col3:
#    compare_conf_table(final_df4,'4 core 8GB')
#  with col4:
#    compare_conf_table(final_df14,'6 core 14GB' )
#  with col5:
#    compare_conf_table(final_df24,'6 core 24GB')
  
  conf_dict = {'5 core 8GB':final_df5,'2 core 8GB':final_df2,'4 core 8GB':final_df4,'6 core 14GB':final_df14,'6 core 24GB':final_df24, '6 core 24GB Exec 1':final_df241}
  col1, col2 = st.columns(2)
  
  with col1:
    conf_1 = st.selectbox('Select First Job Configuration',('5 core 8GB','2 core 8GB','4 core 8GB','6 core 14GB','6 core 24GB', '6 core 24GB Exec 1'))
    compare_conf(conf_dict[conf_1], conf_1)
  
  with col2:
    conf_2 = st.selectbox('Select Second Job Configuration',('5 core 8GB','2 core 8GB','4 core 8GB','6 core 14GB','6 core 24GB', '6 core 24GB Exec 1'))
    compare_conf(conf_dict[conf_2], conf_2)
    
with tab8:
  job_function(final_df241, 6, 24_1)


with tab9:
  # st.write("5 cores")
  # df = final_df5
  # fig = px.line(df, x=['started', "ended", "app_start_time", "app_stop_time"], y="id")
  # fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
  # st.plotly_chart(fig)
  # st.write("2 cores")
  # df = final_df2
  # fig = px.line(df, x=['started', "ended", "app_start_time", "app_stop_time"], y="id")
  # fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
  # st.plotly_chart(fig)
  # st.write("4 cores")
  # df = final_df4
  # fig = px.line(df, x=['started', "ended", "app_start_time", "app_stop_time"], y="id")
  # fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
  # st.plotly_chart(fig)
  # st.write("6 cores 14 Gb")
  # df = final_df14
  # fig = px.line(df, x=['started', "ended", "app_start_time", "app_stop_time"], y="id")
  # fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
  # st.plotly_chart(fig)
  # st.write("6 cores 24 Gb")
  # df = final_df24
  # fig = px.line(df, x=['started', "ended", "app_start_time", "app_stop_time"], y="id")
  # fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
  # st.plotly_chart(fig)
  # st.write("Runtime and WaitTime")
  # df = final_df4
  # fig = px.line(df, x = 'id', y = ['waitTime', 'runtime'], color = 'Error_Type')
  # fig.update_layout(height=500)
  # fig.update_layout(width=800)
  # st.plotly_chart(fig)
  
  st.write("## 6 Core 24 GB memory :red[ 25 Initial Executors]")
  col1, col2 = st.columns(2)
  before_grafana = grafana_df
  before_data = data
  before_grafana['Time'] = pd.to_datetime(before_grafana.Time).dt.tz_localize(None)
  with col1:
    start_date_val = pd.to_datetime(st.date_input("Start Date",datetime.date(2023, 1, 3)))
    start_time_val = st.time_input("Start Time",datetime.time(18, 00))
    final_start_val = datetime.datetime.combine(start_date_val,start_time_val)
  with col2:
    stop_date_val = pd.to_datetime(st.date_input("Stop Date",datetime.date(2023, 1, 6)))
    stop_time_val = st.time_input("Stop Time",datetime.time(18, 00))
    final_stop_val = datetime.datetime.combine(stop_date_val,stop_time_val)
  
  before_grafana = before_grafana[(before_grafana["Time"] > final_start_val) & (before_grafana["Time"] <= final_stop_val)] 
  with col1:
    fig = px.area(before_grafana, x = 'Time', y = 'Value', color = 'Series', color_discrete_sequence=px.colors.qualitative.G10)
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    st.plotly_chart(fig)
  before_grafana = before_grafana[before_grafana.Series == "Compute Nodes (on-demand)"]
  with col2:
    before_data['started'] = pd.to_datetime(before_data.started).dt.tz_localize(None)
    before_data = before_data[(before_data["started"] > final_start_val) & (before_data["started"] <= final_stop_val)]
    with st.expander("Show Data"):
      st.dataframe(before_data)
    st.write(f"**Average Node Count: :red[{round(before_grafana.Value.mean())}]**")
    st.write(f"**Successful Jobs: {len(before_data[before_data.status == 'succeeded'])} jobs**")
    st.write(f"**Failed Jobs: {len(before_data[before_data.status == 'failed'])} jobs**")
    success_data = before_data[before_data.status == 'succeeded']
    st.write(f"**Average Successful Job Runtime: :red[{round(success_data.runtime.mean())} minutes]**")
    fail_data = before_data[before_data.status == 'failed']
    st.write(f"**Average Failed Job Runtime: :red[{round(fail_data.runtime.mean())} minutes]**")
    
  st.write("## 6 Core 24 GB memory :green[ 1 Initial Executors]")
  col1, col2 = st.columns(2)
  after_grafana = grafana_df
  after_data = data
  after_grafana['Time'] = pd.to_datetime(after_grafana.Time).dt.tz_localize(None)
  with col1:
    start_date_val = pd.to_datetime(st.date_input("Start Date",datetime.date(2023, 1, 6)))
    start_time_val = st.time_input("After Change Start Time",datetime.time(18, 15))
    final_start_val = datetime.datetime.combine(start_date_val,start_time_val)
  with col2:
    stop_date_val = pd.to_datetime(st.date_input("Stop Date",datetime.date(2023, 1, 8)))
    stop_time_val = st.time_input("After Change Stop Time",datetime.time(6, 00))
    final_stop_val = datetime.datetime.combine(stop_date_val,stop_time_val)
  after_grafana = after_grafana[(after_grafana["Time"] > final_start_val) & (after_grafana["Time"] <= final_stop_val)]
  after_grafana['Value'] = after_grafana['Value'].apply(lambda x: 50 if x > 50 else x) # Outlier removal
  with col1:
    fig = px.area(after_grafana, x = 'Time', y = 'Value', color = 'Series', color_discrete_sequence=px.colors.qualitative.G10,title="Cluster Infrastructure Usage")
    fig.add_vrect(x0="2023-01-06T18:00:00", x1="2023-01-06T18:30:00",
              annotation_text="Change Implemented",fillcolor="green", opacity=0.25, line_width=2)
    fig.update_layout(height=500)
    fig.update_layout(width=800)
    st.plotly_chart(fig)
    
  after_grafana = after_grafana[after_grafana.Series == "Compute Nodes (on-demand)"]
  st.dataframe(after_grafana)
  with col2:
    after_data['started'] = pd.to_datetime(after_data.started).dt.tz_localize(None)
    after_data = after_data[(after_data["started"] > final_start_val) & (after_data["started"] <= final_stop_val)]
    with st.expander("Show Data"):
      st.dataframe(after_data)
    st.write(f"**Average Node Count: :green[{round(after_grafana.Value.mean())}]**")
    st.write(f"**Successful Jobs: {len(after_data[after_data.status == 'succeeded'])} jobs**")
    st.write(f"**Failed Jobs: {len(after_data[after_data.status == 'failed'])} jobs**")
    success_data = after_data[after_data.status == 'succeeded']
    st.write(f"**Average Successful Job Runtime: :green[{round(success_data.runtime.mean())} minutes]**")
    fail_data = after_data[after_data.status == 'failed']
    st.write(f"**Average Failed Job Runtime :green[{round(fail_data.runtime.mean())} minutes]**")

  