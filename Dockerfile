##### DOCKERFILE FOR IMAGE PYSPARK NOTEBOOK #####
FROM jupyter/pyspark-notebook:python-3.8.8

# create destination directory
RUN mkdir -p /usr/src
WORKDIR /usr/src

# copy files
COPY requirements.txt ./

RUN pip3 install -r requirements.txt

CMD ["streamlit","run", "app.py"]

RUN pip install numpy
RUN pip install pycoingecko
RUN pip install kafka-python
RUN pip install pyspark
RUN pip install pymongo

