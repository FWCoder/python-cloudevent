FROM python:3.6
WORKDIR /usr/src/app
COPY requirements.txt xrayEventServer.py ./
RUN pip install -r requirements.txt
CMD ["python", "-u", "xrayEventServer.py"]