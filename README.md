# Skrawler

**Extract schema metadata of semi-structured files from S3 data lakes.**

`Skrawler` aims to be an open, transparent alternative to AWS Glue's crawler. 

It is perfect if your team wants to build their own data catalogue (+ having it searchable via elastisearch) and where most of your data sits as semi-structured files in S3. Use `skrawler`, Elastisearch, and throw a web app in front of it and you have a data catalogue for your org.

### Features

Simply put, `skrawler` crawls an S3 bucket with a given prefix for all files, extracting its schema and data types, and outputs them to Elastisearch, or as a simple JSON file.

`skrawler` uses the same heuristics as AWS Glue's crawler in how it extracts metadata, namely that it samples the first 1 MB of data only to infer data types.

**Important**: currently only supports `csv` files. `json` and `parquet` are on their way. See below.


## Getting Started

1. `git clone` the repo, then `cd skrawler`.

2. Create a virtual environment using conda or `venv`:

```bash
    conda create --name skrlenv python=3.7
    conda activate skrlenv
```

or with `venv`:

```bash
    python -m venv .
    source .venv/bin/activate
```

3. Install dependencies:

```bash
    pip install -r requirements.txt
```

4. Crawl the heck out of your data lake:

```python
    sklr = S3Crawler(elastisearch_endpoint='http://localhost:9200/', json_output_path='output.json')
    sklr.crawl(bucket='my_bucket', prefix_to_search='prefix/path/')
```

## README!

A README within a README. How quaint.

`skrawler` hopes to emulate AWS Glue's crawler 2 objectives, namely,

1. Extract schema metadata from `csv`, `json`, and `parquet` files.
2. Delineate between the table and partition levels of a data lake.

For `1`, `skrawler` currently only supports `csv`. The other two are in the works.
I'm currently working on further improvements to `1` before beginning work on `2`. If you'd like to help, please do get in touch -- would love some extra pair of hands. The code base is currently small enough that it's easy to get up to speed very quickly.

## Future work

1. Add support for `json` and `parquet`
2. Distinguish partitioning vs table levels
3. Add data sources other than S3
4. Package it