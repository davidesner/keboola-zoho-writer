FROM python:2.7.14
ENV PYTHONIOENCODING utf-8

RUN pip install --no-cache-dir --upgrade --force-reinstall \
		numpy \
		pandas \
		logging_gelf \
		zcrmsdk \
		mysql-connector \	
	&& pip install --no-cache-dir --upgrade --force-reinstall git+git://github.com/keboola/python-docker-application.git
	
COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
