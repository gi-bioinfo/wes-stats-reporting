# Molecular-QC-measures-reporting
## Example run:
```
python get_rdpc.py -p APGI-AU -t ${ego_token} -u https://api.rdpc-qa.cancercollaboratory.org/graphql -r https://github.com/icgc-argo-workflows/rna-seq-alignment -o .
```
## Example getting ongoing running jobs
```
python get_rdpc.py -p P1000-US -u https://api.rdpc-prod.cumulus.genomeinformatics.org/graphql -s RUNNING -r https://github.com/icgc-argo/dna-seq-processing-wfs.git -t ${ego_token} --no_plot
###sftp into server to grab *tasks.tsv.gz
###Assuming we're in directory that contains subdirectory example_dna-seq-processing-wfs_RUNNING_P1000-US or multiple directories with "RUNNING" in the name 
python display_running_jobs.py -i $(pwd)


## Plot reads processed per run
python get_rdpc.py -p P1000-US -t ${ego_token} -u https://api.rdpc-prod.cumulus.genomeinformatics.org/graphql -r https://github.com/icgc-argo/dna-seq-processing-wfs.git -o .
python get_rdpc.py -p MUTO-INTL -t ${ego_token} -u https://api.rdpc-prod.cumulus.genomeinformatics.org/graphql -r https://github.com/icgc-argo/dna-seq-processing-wfs.git -o .
python display_dnaseq_reads_processed.py -t example/dna-seq-processing-wfs_P1000-US_COMPLETE_runs.tsv.gz example/dna-seq-processing-wfs_MUTO-INTL_COMPLETE_runs.tsv.gz
```


## Requirements:
[numpy](https://anaconda.org/anaconda/numpy)<Br>
[pandas](https://anaconda.org/anaconda/pandas)<Br>
[plotly](https://anaconda.org/conda-forge/plotly)<Br>
[pickle](https://anaconda.org/conda-forge/pypickle/files)<Br>
[kaleido](https://anaconda.org/conda-forge/python-kaleido)<Br>
[oauthlib](https://anaconda.org/conda-forge/oauthlib)<Br>
[requests-oauthlib](https://anaconda.org/conda-forge/requests-oauthlib)<Br>
[dotenv](https://anaconda.org/conda-forge/python-dotenv)<Br>
