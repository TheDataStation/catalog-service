# Data Asset Catalog (DAC) Service

## Rationale

### Responsibilities
* maintains the lifecycle and metadata—how
data came to be, how it changed, how it’s been used—of each dataset
and derived dataset hosted in the Station. 
* serves the  discovery and integration engine, by data users to describe capsule’s
trust constraints, and by contributors and stewards to implement
access and governance policies.

### Mental Model
DAC implements a common mental model
consisting of **profiles** and **relationships**.

**Profiles** are descriptions
of the data, such as statistical distribution of values, sketches, but
also temporal information and others. 

**Profiles** are automatically
computed by the `Metadata Engine` when datasets are submitted to
the Station, or elicited from humans using incentives.

**Profiles** include: 
* what-profile  to describe an actual dataset
* how-profile to indicate what program produced the **current** dataset version,
* who-profile to indicate who
produced and who uses the dataset
* where-profile to indicate how
the dataset can be accessed
* why-profile to explain the purpose
of the dataset
* when-profile to explain when the dataset was modified and when it is valid. 
  
**Relationships** are built out of profiles:
for example, provenance is built from who- and how-profiles, and
syntactic relationships such as join and similarity graphs are built
from what-profiles. 

Both profiles and relationships are used by the
discovery and integration modules.

DAC’s logical schema design strives for a balance between
* **structuredness**, which facilitates querying, and
* **flexibility**, which facilitates including new data. 
  
At its core, it reflects the mental
model introduced above, which allows different parties to 
understand and query it effectively. 

To increase flexibility, it supports
semi-structured data, such as JSON, to reflect the idiosyncrasies of
different data formats: e.g., describing an image is different than
describing a relation or a ML model.

## Populating the Catalog. 

The Data Station triggers the execution
of a `Metadata Engine` whenever a new dataset is received, an existing
dataset is updated, or the Station produces a derived data product
from existing ones. 

The `Metadata Engine` analyzes the dataset and
extracts as many profiles as it can automatically. 

This is done via the
orchestration of analyzers that specialize in different profiles, but
also by eliciting this information from data users and contributors
directly when it cannot be accessed differently (see Section 4. As a
consequence, the full lifecyle of residing datasets and derived data
products is known because all operations on that dataset happen
within the realm of the Station.

## Catalog Service. 

A catalog service facilitates loading and querying, and stores and enforces access and governance policies. The
service maintains schemas of the semi-structured data as well as
indexes that permit the discovery and blending engines to find the
information they need, and data users to specify trust constraints
to guarantee the origin and nature of the results they request.

## Setting up the development environment

Setting up the environment is normally done via GNU `make` 

Start by creating an environment with `conda` (assuming it's already installed)

`make create-environment`

Base environment is declared in `environment.yml`.
`pip`-managed requirements are handled in `requirements*.txt` files.
These files are read both by `environment.yml` and by the `setup.py` script.

Subsequent changes in the environment, can be tracked & managed via other `make` targets accordingly
* `make update-environment`
* `make list-outdated-packages`