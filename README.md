# Data México Data ETL

The purpose of this repository is to have a single source of truth for the data ETL pipeline. This pipeline will begin with a variety of sources (single text files, APIs etc) that will be integrated using the [Bamboo Python library](https://github.com/Datawheel/bamboo-lib) and ultimately ingested into the Data México database.

## Workflow

Since we'll be working in a large team it'll be important for us to always be working from the latest stable version of the project. To do this, we will always commit our work to a feature branch and submit pull requests that will then be merged to the master branch by the repository owner. Here's a detailed article on this workflow: https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow

### Example workflow

Once you have a local copy of the repository on your machine, the following steps will enable you to commit code to the repository:

Create a new branch:

```commandline
$ git checkout -b new-feature
```

...do your work, edit files, add new ones etc...

Update, add, commit, and push changes:
```commandline
git status
git add <files>
git commit -m "adds better documentation to exports data"
```

Push feature branch to remote when all changes are commited

```commandline
git push -u origin new-feature
```

## Setup

*incomplete! ...*

### 1. Clone the repo

```commandline
$ git clone https://github.com/datamexico/data-etl.git
$ cd data-etl
$ git checkout -b new-feature
```

### 2. Add any environment variables

Use the following as a guide/template for a `.env` file:

```

```

### 3. Create the database


### 4. Run the pipelines

To run all of the pipelines defined under `/etl`:

```commandline
$ python pipelines.py
```

You can also choose to run each of them individually. For example:

```commandline
$ python etl/geos/geos.py
```
