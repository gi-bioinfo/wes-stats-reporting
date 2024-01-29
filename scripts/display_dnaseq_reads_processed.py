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
import sys
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
    parser.add_argument('-t', '--tsv', dest="tsv", help="location of TSVs from get_rdpc.py", required=True,type=str,nargs="+")
    parser.add_argument('-u', '--url', dest="url", help="ARGO song URL", default="https://song.rdpc-prod.cumulus.genomeinformatics.org",type=str)
    parser.add_argument('-o', '--output_directory', dest="out_dir", help="location to save images", default=os.getcwd(),type=str)

    cli_input= parser.parse_args()

    process_df,task_df=combine_tasks(cli_input.tsv)

    calculate_task_times(process_df,task_df)

    get_lib_depth(cli_input.url,process_df)

    plots={}

    colors=get_process_colors(process_df)

    for count,col in  enumerate(['bwa','merSorMkdup','laneBam','scoreDn','scoreUp','alignedSeqQC','rgQC','oxog']):
        plots["fig.%sA_%s_tasks" % (str(count+1),col)]=reads_processed_hr_job(process_df,col,colors)
        plots["fig.%sB_%s_tasks" % (str(count+1),col)]=reads_processed_hr_project(process_df,col,colors)

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



def combine_tasks(tsv_paths):

    list_of_jobs=[]
    list_of_tasks=[]

    for tsv in tsv_paths:
        if os.path.exists(tsv):
            list_of_jobs.append(tsv)
        else:
            sys.exit("%s not found" % (tsv))

        if os.path.exists(tsv.replace("_runs.tsv.gz","_tasks.tsv.gz")):
            list_of_tasks.append(tsv.replace("_runs.tsv.gz","_tasks.tsv.gz"))
        else:
            sys.exit("%s not found" % (tsv.replace("_runs.tsv.gz","_tasks.tsv.gz")))

    if len(list_of_jobs)==0:
    	exit("Nothing to process. No TSVs found")

    print("Read and combining runs...")
    jobs=\
    pd.concat([pd.read_csv(tsv,sep='\t',compression='gzip').assign(timestamp=tsv.replace("_runs.tsv.gz","").split("/")[-1].split("_")[-1]) for tsv in list_of_jobs])\
    .reset_index()\
    .iloc[:,2:]
    print("Read and combining runs...Complete")
    print("Read and combining tasks...")
    tasks=\
    pd.concat([pd.read_csv(tsv,sep='\t',compression='gzip').assign(timestamp=tsv.replace("_tasks.tsv.gz","").split("/")[-1].split("_")[-1]) for tsv in list_of_tasks])\
    .reset_index()\
    .iloc[:,2:]

    print("Read and combining runs...Complete")
    return(jobs,tasks)

def calculate_task_times(runs,tasks):
    print("Aggregating task times...")
    for ind in runs.index.values.tolist():
        runId=runs.loc[ind,"runId"]

        for simple_name,process_name in [
            ["bwa",'DnaAln:bwaMemAligner'],
            ["merSorMkdup",'DnaAln:merSorMkdup'],
            ["laneBam",'DnaAln:toLaneBam'],
            ["scoreDn",'DnaAln:dnld:scoreDn'],
            ["scoreUp",'DnaAln:upAln:scoreUp'],
            ["alignedSeqQC",'DnaAln:alignedSeqQC'],
            ["rgQC",'DnaAln:rgQC'],
            ["oxog",'DnaAln:oxog']
        ]:
            tmp=tasks.query("runId==@runId and process==@process_name")['realtime'].values.tolist()
            if len(tmp)==0:
                runs.loc[ind,"%s_time" % (simple_name)]=None
            else:
                runs.loc[ind,"%s_time" % (simple_name)]=sum(tmp)

            tmp=tasks.query("runId==@runId and process==@process_name")['cpus'].unique().tolist()
            if len(tmp)==0:
                runs.loc[ind,"%s_cpus" % (simple_name)]=None
            else:
                runs.loc[ind,"%s_cpus" % (simple_name)]=tmp[0]

            tmp=tasks.query("runId==@runId and process==@process_name")['memory_gb'].unique().tolist()
            if len(tmp)==0:
                runs.loc[ind,"%s_mem" % (simple_name)]=None
            else:
                runs.loc[ind,"%s_mem" % (simple_name)]=tmp[0]

    print("Aggregating task times...Done")


def get_lib_depth(url,runs):
    for ind in runs.index.values.tolist():
        if ind%100==0:
            print("Calling RDPC for...%s/%s" % (str(ind+1),str(len(runs))))
                         
        study_id=runs.loc[ind,"study_id"]
        sample_id=runs.loc[ind,"sample_id"]
        
        endpoint="%s/studies/%s/analysis/search/id?sampleId=%s" % (url,study_id,sample_id)
        
        get_response=requests.get(endpoint)
        if get_response.status_code!=200:
            print("Query response failed, return status_code :%s" % response.status_code)
            break
        
        counts=[]
        for z in get_response.json():
            if z['analysisType']['name']=='qc_metrics' and z['workflow']['workflow_name']=='DNA Seq Alignment':
                for file in z['files']:
                    if file.get("info").get("metrics"):
                        if file.get("info").get("analysis_tools")[0]=='Picard:CollectQualityYieldMetrics':
                            counts.append(int(file['info']['metrics']['total_reads']))
                            
            if z['analysisType']['name']=='qc_metrics' and z['workflow']['workflow_name']=='DNA Seq Alignment':
                for file in z['files']:
                    if file.get("info").get("metrics"):
                        if file.get("info").get("analysis_tools")[0]=='Samtools:stats':
                            runs.loc[ind,"map_percent"]=int(file['info']['metrics']['mapped_reads'])/int(file['info']['metrics']['total_reads'])
                            runs.loc[ind,"error_rate"]=float(file['info']['metrics']["error_rate"])
        
        runs.loc[ind,"num_readgroups"]=len(counts)
        runs.loc[ind,"total_reads"]=sum(counts)

def reads_processed_hr_job(runs,col,colors):
    fig=plotly.subplots.make_subplots(
        cols=1,
        rows=1
    )

    tmp=runs.loc[:,["study_id","runId",'%s_mem' % (col),'%s_cpus' % (col),'%s_time' % (col),"total_reads"]]

    for ind in tmp.index.values.tolist():
        if tmp.loc[ind,'total_reads']==0 or runs.loc[ind,'%s_time' % (col)]==0:
            continue
        else:
            tmp.loc[ind,'reads_per_hr']=tmp.loc[ind,'total_reads']/tmp.loc[ind,'%s_time' % (col)]


    count=0
    for study_id,mem,cpus in tmp.replace(np.NaN,0).query("reads_per_hr!=0")\
    .groupby(['study_id','%s_mem' % (col),'%s_cpus' % (col)])\
    .agg({
        "total_reads":["mean","median","count"]
    }
    ).reset_index().loc[:,["study_id",'%s_mem' % (col),'%s_cpus' % (col)]].values.tolist():
        fig.append_trace(
                    go.Scatter(
                        x=tmp.replace(np.NaN,0).query("reads_per_hr!=0").query("study_id==@study_id and %s_mem==@mem and %s_cpus==@cpus" % (col,col)).sort_values("reads_per_hr")['runId'].values.tolist(),
                        y=tmp.replace(np.NaN,0).query("reads_per_hr!=0").query("study_id==@study_id and %s_mem==@mem and %s_cpus==@cpus" % (col,col)).sort_values("reads_per_hr")['reads_per_hr'].values.tolist(),
                        showlegend=True,
                        name="%s mem:%s cpus:%s" % (study_id,str(mem),str(cpus)),
                        mode="markers"
                    ),1,1)

    fig['layout'].update(
    width=1000,
    height=600,
    title="Comparing %s jobs" % (col),
    xaxis=dict(title="run"),
    yaxis=dict(title="ReadsAlignedPerHour"),
    showlegend=True,
    titlefont=dict(size=20),
    legend=go.layout.Legend(orientation='h',font=dict(size=20))
    )
    fig['layout']['xaxis1'].update(showticklabels=False)
    return(fig)


def reads_processed_hr_project(runs,col,colors):
    fig=plotly.subplots.make_subplots(
        cols=1,
        rows=1
    )

    for study_id in runs['study_id'].unique().tolist():
        tmp=runs.query("study_id==@study_id").loc[:,["total_reads",'%s_time' % (col)]]

        for ind in tmp.index.values.tolist():
            if tmp.loc[ind,'total_reads']==0 or runs.loc[ind,'%s_time' % (col)]==0:
                continue
            else:
                tmp.loc[ind,'reads_per_hr']=tmp.loc[ind,'total_reads']/tmp.loc[ind,'%s_time' % (col)]

        fig.append_trace(
                    go.Violin(
                        y=tmp['reads_per_hr'].values.tolist(),
                        showlegend=False,
                        box_visible=True,
                        line_color='black',
                        fillcolor=colors[study_id]['rgb'].replace(")",", 0.2)").replace("rgb","rgba"),
                        x0=study_id
                    ),1,1)


    fig['layout'].update(
        width=600,
        height=600,
        title="Comparing processing times %s for projects" % (col),
        yaxis=dict(title="ReadsAlignedPerHour"),
        showlegend=True,
        titlefont=dict(size=20),
    )
    return(fig)

def get_process_colors(dataframe):
    process_dict={key:{"hex":plotly.colors.qualitative.Plotly[count],"rgb":plotly.colors.convert_colors_to_same_type(plotly.colors.qualitative.Plotly[count],colortype='rgb')[0][0]} for count,key in enumerate(dataframe['study_id'].unique().tolist())}
    return(process_dict)

if __name__ == "__main__":
    main()
