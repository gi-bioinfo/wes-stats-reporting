#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
  Copyright (C) 2022,  icgc-argo

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  Authors:
    Edmund Su
"""

import pandas as pd
import requests
import numpy as np
import os
import argparse
import plotly
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import glob
import pickle

def main():
    """

    """
    parser = argparse.ArgumentParser(description='Utilize stats from SONG API to generate plots about running jobs')
    parser.add_argument('-i', '--input_directory', dest="input_dir", help="location to scrap for runnings tasks", default=os.getcwd(),type=str)
    parser.add_argument('-o', '--output_directory', dest="out_dir", help="location to save images", default=os.getcwd(),type=str)

    cli_input= parser.parse_args()

    tasks_df=combine_tasks(cli_input.input_dir)

    status_dict=get_task_status_colors()
    process_dict=get_process_colors(tasks_df)

    plots={}
    latest=tasks_df.sort_values('timestamp')['timestamp'].unique().tolist()[-1]

    plots["%s_job_statuses" % (latest)]=plot_status(tasks_df,status_dict)

    for state in ["RUNNING","QUEUED","EXECUTOR_ERROR","COMPLETE"]:
        plots["%s_%s_tasks" % (latest,state)]=plot_process(tasks_df,process_dict,state)


    save_pkl_plots(cli_input.out_dir,plots)


def save_pkl_plots(out_dir,generated_plots):
    svg_dir="%s/%s" % (out_dir,"svg")
    pkl_dir="%s/%s" % (out_dir,"pkl")

    if not os.path.exists(svg_dir):
        os.makedirs(svg_dir)
    if not os.path.exists(pkl_dir):
        os.makedirs(pkl_dir)

    print("Saving plots...")
    for generated_plot in generated_plots.keys():
        file = open("%s/%s.pkl" % (pkl_dir,generated_plot),"wb")
        pickle.dump(generated_plots[generated_plot],file)
        file.close()
    print("Saving plots...Complete")

    print("Saving plots SVGs...")
    for generated_plot in generated_plots.keys():
        generated_plots[generated_plot].write_image("%s/%s.svg" % (svg_dir,generated_plot))
    print("Saving plots SVGs...Complete")

def plot_process(task_dataframe,process_dict,state):
    fig=plotly.subplots.make_subplots(cols=1,rows=1)
    showlegend=True

    for timestamp in task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist():
        for process in process_dict.keys():
            fig.append_trace(
                        go.Bar(
                            y=[len(task_dataframe.query("timestamp==@timestamp and state==@state and process==@process"))],
                            x=[timestamp],
                            name=process,
                            marker={"color":process_dict[process]},
                            showlegend=showlegend
                        ),1,1)
        showlegend=False
    
    earliest=task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()[0]
    latest=task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()[-1]

    fig['layout'].update(
    width=1000 if 50*len(task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist())<1000 else 50*len(task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()),
    height=600,
    title="%s jobs : %s to %s" % (state,earliest,latest),
    showlegend=True,
    titlefont=dict(size=20),
    legend=go.layout.Legend(font=dict(size=15)),
    barmode='stack'
    )
    fig['layout']['yaxis1'].update(title="# Tasks")
    fig['layout']['xaxis1'].update(title="Timestamp",type='category')

    return(fig)
def plot_status(task_dataframe,status_dict):
	fig=plotly.subplots.make_subplots(cols=1,rows=1)
	showlegend=True

	for timestamp in task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist():
	    for state in ["RUNNING","QUEUED","EXECUTOR_ERROR"]:
	        fig.append_trace(
	                    go.Bar(
	                        y=[len(task_dataframe.query("timestamp==@timestamp and state==@state"))],
	                        x=[str(timestamp)],
	                        name=state,
	                        marker={"color":status_dict[state]},
	                        showlegend=showlegend
	                    ),1,1)
	    showlegend=False

	earliest=task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()[0]
	latest=task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()[-1]

	fig['layout'].update(
	    width=1000 if 50*len(task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist())<1000 else 50*len(task_dataframe.sort_values('timestamp')['timestamp'].unique().tolist()),
	    height=600,
	    title="Distribution of task statuses :%s to %s" % (earliest,latest),
	    showlegend=True,
	    titlefont=dict(size=20),
	    legend=go.layout.Legend(font=dict(size=20)),
	    barmode='stack'
	)
	fig['layout']['yaxis1'].update(title="# jobs")
	fig['layout']['xaxis1'].update(title="timestamp",type='category')

	return(fig)

def combine_tasks(input_dir):
    list_of_tsvs=[]
    for working_dir in glob.iglob("%s/*RUNNING*" % input_dir):
        for tsv in glob.iglob("%s/tsv/*_tasks.tsv.gz" % working_dir):
            list_of_tsvs.append(tsv)

    if len(list_of_tsvs)==0:
    	exit("Nothing to process. No TSVs found")
    tmp=\
    pd.concat([pd.read_csv(tsv,sep='\t',compression='gzip').assign(timestamp=tsv.replace("_tasks.tsv.gz","").split("/")[-1].split("_")[-1]) for tsv in list_of_tsvs])\
    .reset_index()\
    .iloc[:,2:]

    return(tmp)

def get_process_colors(dataframe):
    process_dict={key:plotly.colors.qualitative.Dark24[count] for count,key in enumerate(dataframe['process'].unique().tolist())}
    return(process_dict)


def get_task_status_colors():
    tmp_dict={
    "RUNNING":"green",
    "QUEUED":"grey",
    "EXECUTOR_ERROR":"black"
    }	
    return(tmp_dict)


if __name__ == "__main__":
    main()
