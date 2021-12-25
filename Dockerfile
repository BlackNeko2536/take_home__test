FROM jupyter/pyspark-notebook:latest
RUN conda install -c anaconda psycopg2
RUN conda install -c anaconda sqlalchemy
RUN conda install -c anaconda pandas
RUN conda install -c conda-forge findspark