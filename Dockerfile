FROM debian:bullseye

RUN apt-get update && apt-get install -y python3

RUN python3 --version


RUN apt-get update && apt-get install -y curl

RUN curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

RUN bash Miniconda3-latest-Linux-x86_64.sh -b -p /conda

RUN apt-get update && apt-get install -y make

ADD . /catalog-service

WORKDIR /catalog-service

ENV PATH=/conda/bin:$PATH

RUN make create-environment

# this is approximately the same as activating the
# conda environment, but more amenable to Dockerfile
ENV PATH=/conda/envs/ds-catalog-service/bin:$PATH

