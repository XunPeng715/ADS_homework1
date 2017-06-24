FROM python

COPY *.py *.json *.sh *.csv /

RUN pip install requests

RUN pip install jupyter

RUN pip install pandas

RUN pip install boto3

RUN pip install botocore

CMD ["python", "Wrangle.py"] 